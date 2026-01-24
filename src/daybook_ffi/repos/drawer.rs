use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use daybook_core::drawer::{DocNBranches, DrawerEvent, DrawerRepo, UpdateDocArgs};
use daybook_types::doc::{AddDocArgs, ChangeHashSet, Doc, DocId, DocPatch};

#[derive(uniffi::Object)]
struct DrawerRepoFfi {
    fcx: SharedFfiCtx,
    repo: Arc<DrawerRepo>,
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
    #[tracing::instrument(err, skip(fcx))]
    async fn load(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let fcx = Arc::clone(&fcx);
        let (repo, stop_token) = fcx
            .do_on_rt(DrawerRepo::load(
                fcx.cx.acx.clone(),
                fcx.cx.doc_drawer().document_id().clone(),
                fcx.cx.local_actor_id.clone(),
            ))
            .await
            .inspect_err(|err| tracing::error!(?err))?;
        Ok(Arc::new(Self {
            fcx,
            repo,
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
    }

    #[tracing::instrument(err, skip(self))]
    async fn get(self: Arc<Self>, id: DocId, branch_path: String) -> Result<Option<Doc>, FfiError> {
        let this = Arc::clone(&self);
        let branch_path = daybook_types::doc::BranchPath::from(branch_path);
        Ok(self
            .fcx
            .do_on_rt(async move {
                this.repo
                    .get(&id, &branch_path)
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
            .do_on_rt(async move { this.repo.add(args).await })
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
                    .wrap_err("error applying patches")
            })
            .await?)
    }

    #[tracing::instrument(err, skip(self))]
    async fn del(self: Arc<Self>, id: DocId) -> Result<bool, FfiError> {
        let this = Arc::clone(&self);
        Ok(self
            .fcx
            .do_on_rt(async move { this.repo.del(&id).await })
            .await?)
    }
}
