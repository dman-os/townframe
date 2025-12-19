use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use daybook_core::drawer::{DrawerEvent, DrawerRepo};
use daybook_core::gen::doc::{Doc, DocId, DocPatch};

#[derive(uniffi::Object)]
struct DrawerRepoFfi {
    fcx: SharedFfiCtx,
    repo: Arc<DrawerRepo>,
}

impl daybook_core::repos::Repo for DrawerRepoFfi {
    type Event = DrawerEvent;
    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.repo.registry
    }
}

crate::uniffi_repo_listeners!(DrawerRepoFfi, DrawerEvent);

#[uniffi::export]
impl DrawerRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx))]
    async fn load(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let fcx = fcx.clone();
        let repo = fcx
            .do_on_rt(DrawerRepo::load(
                fcx.cx.acx.clone(),
                fcx.cx.doc_drawer().document_id().clone(),
            ))
            .await
            .inspect_err(|err| tracing::error!(?err))?;
        Ok(Arc::new(Self { fcx, repo }))
    }

    // old FFI wrappers for contains/insert/remove removed; use `ffi_get`, `ffi_add`, `ffi_update`, `ffi_del` instead
    #[tracing::instrument(skip(self))]
    async fn list(self: Arc<Self>) -> Vec<DocId> {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.list().await })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    async fn get(self: Arc<Self>, id: DocId) -> Result<Option<Doc>, FfiError> {
        let this = self.clone();
        Ok(self
            .fcx
            .do_on_rt(async move {
                this.repo
                    .get(&id)
                    .await
                    .map(|opt| opt.map(|arc| (*arc).clone()))
            })
            .await?)
    }

    #[tracing::instrument(err, skip(self))]
    async fn add(self: Arc<Self>, doc: Doc) -> Result<DocId, FfiError> {
        let this = self.clone();
        Ok(self
            .fcx
            .do_on_rt(async move { this.repo.add(doc).await })
            .await?)
    }

    // singular update removed; expose batch-only API

    #[tracing::instrument(err, skip(self))]
    async fn update_batch(self: Arc<Self>, docs: Vec<DocPatch>) -> Result<(), FfiError> {
        let this = self.clone();
        Ok(
            self
            .fcx
            .do_on_rt(async move { this.repo.update_batch(docs).await })
            .await?
        )
    }

    #[tracing::instrument(err, skip(self))]
    async fn del(self: Arc<Self>, id: DocId) -> Result<bool, FfiError> {
        let this = self.clone();
        Ok(self
            .fcx
            .do_on_rt(async move { this.repo.del(&id).await })
            .await?)
    }
}
