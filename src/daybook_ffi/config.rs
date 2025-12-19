use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use daybook_core::config::{ConfigEvent, ConfigRepo};

#[derive(uniffi::Object)]
struct ConfigRepoFfi {
    fcx: SharedFfiCtx,
    repo: Arc<ConfigRepo>,
}

impl daybook_core::repos::Repo for ConfigRepoFfi {
    type Event = ConfigEvent;
    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.repo.registry
    }
}

crate::uniffi_repo_listeners!(ConfigRepoFfi, ConfigEvent);

#[uniffi::export]
impl ConfigRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx))]
    async fn load(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let fcx = fcx.clone();
        let repo = fcx
            .do_on_rt(ConfigRepo::load(
                fcx.cx.acx.clone(),
                fcx.cx.doc_app().document_id().clone(),
            ))
            .await
            .inspect_err(|err| tracing::error!(?err))?;
        Ok(Arc::new(Self { fcx, repo }))
    }

}
