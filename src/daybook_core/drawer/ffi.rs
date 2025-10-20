use super::*;

#[derive(uniffi::Object)]
struct DrawerRepoFfi {
    fcx: SharedFfiCtx,
    repo: DrawerRepo,
}

impl crate::repos::Repo for DrawerRepoFfi {
    type Event = DrawerEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
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
    async fn ffi_list(self: Arc<Self>) -> Vec<DocId> {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.list().await })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_get(self: Arc<Self>, id: DocId) -> Result<Option<Doc>, FfiError> {
        let this = self.clone();
        Ok(self
            .fcx
            .do_on_rt(async move { this.repo.get(id).await })
            .await?)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_add(self: Arc<Self>, doc: Doc) -> Result<DocId, FfiError> {
        let this = self.clone();
        Ok(self
            .fcx
            .do_on_rt(async move { this.repo.add(doc).await })
            .await?)
    }

    // singular update removed; expose batch-only API

    #[tracing::instrument(err, skip(self))]
    async fn ffi_update_batch(self: Arc<Self>, docs: Vec<DocPatch>) -> Result<(), FfiError> {
        let this = self.clone();
        Ok(self
            .fcx
            .do_on_rt(async move { this.repo.update_batch(docs).await })
            .await?)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_del(self: Arc<Self>, id: DocId) -> Result<bool, FfiError> {
        let this = self.clone();
        Ok(self
            .fcx
            .do_on_rt(async move { this.repo.del(id).await })
            .await?)
    }
}
