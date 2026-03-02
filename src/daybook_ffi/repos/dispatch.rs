use crate::ffi::{FfiError, SharedFfiCtx};
use crate::interlude::*;

use daybook_core::rt::dispatch::{DispatchEvent, DispatchRepo};

#[derive(uniffi::Object)]
pub struct DispatchRepoFfi {
    fcx: SharedFfiCtx,
    pub repo: Arc<DispatchRepo>,
    stop_token: tokio::sync::Mutex<Option<daybook_core::repos::RepoStopToken>>,
}

impl daybook_core::repos::Repo for DispatchRepoFfi {
    type Event = DispatchEvent;

    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.repo.registry
    }

    fn cancel_token(&self) -> &tokio_util::sync::CancellationToken {
        self.repo.cancel_token()
    }
}

crate::uniffi_repo_listeners!(DispatchRepoFfi, DispatchEvent);

#[uniffi::export]
impl DispatchRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx))]
    pub async fn load(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let (repo, stop_token) = fcx
            .do_on_rt(DispatchRepo::load(
                fcx.rcx.acx.clone(),
                fcx.rcx.doc_app.document_id().clone(),
                fcx.rcx.local_actor_id.clone(),
            ))
            .await
            .inspect_err(|err| tracing::error!(?err))?;
        Ok(Arc::new(Self {
            fcx,
            repo,
            stop_token: Some(stop_token).into(),
        }))
    }

    pub async fn stop(&self) -> Result<(), FfiError> {
        if let Some(token) = self.stop_token.lock().await.take() {
            token.stop().await?;
        }
        Ok(())
    }

    pub async fn list(self: Arc<Self>) -> Result<Vec<String>, FfiError> {
        let this = Arc::clone(&self);
        let ids = self
            .fcx
            .do_on_rt(async move {
                this.repo
                    .list()
                    .await
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect::<Vec<_>>()
            })
            .await;
        Ok(ids)
    }
}
