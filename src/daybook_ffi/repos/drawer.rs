use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};
use crate::repos::plugs::PlugsRepoFfi;

use daybook_core::drawer::types::UpdateDocArgsV2 as UpdateDocArgs;
use daybook_core::drawer::{DocNBranches, DrawerEvent, DrawerRepo};
use daybook_types::doc::{AddDocArgs, ChangeHashSet, Doc, DocId, DocPatch};

#[derive(uniffi::Object)]
struct DrawerRepoFfi {
    fcx: SharedFfiCtx,
    repo: Arc<DrawerRepo>,
    _plugs_repo: Arc<PlugsRepoFfi>,
    stop_token: tokio::sync::Mutex<Option<daybook_core::repos::RepoStopToken>>,
}

impl daybook_core::repos::Repo for DrawerRepoFfi {
    type Event = DrawerEvent;
    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.repo.registry
    }

    fn cancel_token(&self) -> &tokio_util::sync::CancellationToken {
        self.repo.cancel_token()
    }
}

crate::uniffi_repo_listeners!(DrawerRepoFfi, DrawerEvent);

#[uniffi::export]
impl DrawerRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx, plugs_repo))]
    async fn load(fcx: SharedFfiCtx, plugs_repo: Arc<PlugsRepoFfi>) -> Result<Arc<Self>, FfiError> {
        let fcx = Arc::clone(&fcx);
        let cx = Arc::clone(fcx.repo_ctx());
        let (repo, stop_token) = fcx
            .do_on_rt(DrawerRepo::load(
                cx.acx().clone(),
                cx.doc_drawer().document_id().clone(),
                cx.local_actor_id().clone(),
                Arc::new(std::sync::Mutex::new(
                    daybook_core::drawer::lru::KeyedLruPool::new(1000),
                )),
                Arc::new(std::sync::Mutex::new(
                    daybook_core::drawer::lru::KeyedLruPool::new(1000),
                )),
            ))
            .await
            .inspect_err(|err| tracing::error!(?err))?;
        repo.set_plugs_repo(Arc::clone(&plugs_repo.repo));
        Ok(Arc::new(Self {
            fcx,
            repo,
            _plugs_repo: plugs_repo,
            stop_token: Some(stop_token).into(),
        }))
    }

    async fn stop(&self) -> Result<(), FfiError> {
        if let Some(token) = self.stop_token.lock().await.take() {
            token.stop().await?;
        }
        Ok(())
    }

    // old FFI wrappers for contains/insert/remove removed; use `ffi_get`, `ffi_add`, `ffi_update`, `ffi_del` instead
    #[tracing::instrument(skip(self))]
    async fn list(self: Arc<Self>) -> Vec<DocNBranches> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move { this.repo.list().await })
            .await
            .expect("error listing docs")
    }

    #[tracing::instrument(err, skip(self))]
    async fn get(self: Arc<Self>, id: DocId, branch_path: String) -> Result<Option<Doc>, FfiError> {
        let this = Arc::clone(&self);
        let branch_path = daybook_types::doc::BranchPath::from(branch_path);
        Ok(self
            .fcx
            .do_on_rt(async move {
                this.repo
                    .get_doc_with_facets_at_branch(&id, &branch_path, None)
                    .await
                    .map(|opt| opt.map(|arc| (*arc).clone()))
            })
            .await?)
    }

    #[tracing::instrument(err, skip(self))]
    async fn add(self: Arc<Self>, args: AddDocArgs) -> Result<DocId, FfiError> {
        let this = Arc::clone(&self);
        Ok(self
            .fcx
            .do_on_rt(async move { this.repo.add(args).await.map_err(eyre::Report::from) })
            .await?)
    }

    #[tracing::instrument(err, skip(self))]
    async fn update(
        self: Arc<Self>,
        patch: DocPatch,
        branch_path: String,
        heads: Option<ChangeHashSet>,
    ) -> Result<(), FfiError> {
        let this = Arc::clone(&self);
        let branch_path = daybook_types::doc::BranchPath::from(branch_path);
        Ok(self
            .fcx
            .do_on_rt(async move {
                this.repo
                    .update_at_heads(patch, branch_path, heads)
                    .await
                    .wrap_err("error applying patch")
            })
            .await?)
    }

    #[tracing::instrument(err, skip(self))]
    async fn update_batch(self: Arc<Self>, patches: Vec<UpdateDocArgs>) -> Result<(), FfiError> {
        let this = Arc::clone(&self);
        Ok(self
            .fcx
            .do_on_rt(async move {
                this.repo
                    .update_batch(patches)
                    .await
                    .inspect_err(|err| error!(?err, "XXX"))
                    .wrap_err("error applying patches")
            })
            .await?)
    }

    #[tracing::instrument(err, skip(self))]
    async fn del(self: Arc<Self>, id: DocId) -> Result<bool, FfiError> {
        let this = Arc::clone(&self);
        Ok(self
            .fcx
            .do_on_rt(async move { this.repo.del(&id).await.map_err(eyre::Report::from) })
            .await?)
    }
}
