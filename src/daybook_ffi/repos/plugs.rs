use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};
use daybook_core::plugs::{PlugsEvent, PlugsRepo};

#[derive(uniffi::Object)]
pub struct PlugsRepoFfi {
    _fcx: SharedFfiCtx,
    pub repo: Arc<PlugsRepo>,
    stop_token: tokio::sync::Mutex<Option<daybook_core::repos::RepoStopToken>>,
}

impl daybook_core::repos::Repo for PlugsRepoFfi {
    type Event = PlugsEvent;
    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.repo.registry
    }

    fn cancel_token(&self) -> &tokio_util::sync::CancellationToken {
        self.repo.cancel_token()
    }
}

crate::uniffi_repo_listeners!(PlugsRepoFfi, PlugsEvent);

#[uniffi::export]
impl PlugsRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx, blobs_repo))]
    async fn load(
        fcx: SharedFfiCtx,
        blobs_repo: Arc<crate::repos::blobs::BlobsRepoFfi>,
    ) -> Result<Arc<Self>, FfiError> {
        let fcx = Arc::clone(&fcx);
        let (repo, stop_token) = fcx
            .do_on_rt(PlugsRepo::load(
                fcx.cx.acx.clone(),
                Arc::clone(&blobs_repo.repo),
                fcx.cx.doc_app().document_id().clone(),
                fcx.cx.local_actor_id.clone(),
            ))
            .await
            .inspect_err(|err| tracing::error!(?err))?;
        Ok(Arc::new(Self {
            _fcx: fcx,
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
}
