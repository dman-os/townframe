use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};
use daybook_core::plugs::{PlugsEvent, PlugsRepo};

#[derive(uniffi::Object)]
pub struct PlugsRepoFfi {
    fcx: SharedFfiCtx,
    pub repo: Arc<PlugsRepo>,
}

impl daybook_core::repos::Repo for PlugsRepoFfi {
    type Event = PlugsEvent;
    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.repo.registry
    }
}

crate::uniffi_repo_listeners!(PlugsRepoFfi, PlugsEvent);

#[uniffi::export]
impl PlugsRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx))]
    async fn load(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let fcx = fcx.clone();
        let repo = fcx
            .do_on_rt(PlugsRepo::load(
                fcx.cx.acx.clone(),
                fcx.cx.doc_app().document_id().clone(),
            ))
            .await
            .inspect_err(|err| tracing::error!(?err))?;
        Ok(Arc::new(Self { fcx, repo }))
    }
}
