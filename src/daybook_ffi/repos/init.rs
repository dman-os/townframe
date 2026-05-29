use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use daybook_core::rt::init::{InitEvent, InitRepo};

#[derive(uniffi::Object)]
pub struct InitRepoFfi {
    _fcx: SharedFfiCtx,
    pub repo: Arc<InitRepo>,
    stop_token: tokio::sync::Mutex<Option<daybook_core::repos::RepoStopToken>>,
}

impl daybook_core::repos::Repo for InitRepoFfi {
    type Event = InitEvent;
    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.repo.registry
    }

    fn cancel_token(&self) -> &tokio_util::sync::CancellationToken {
        self.repo.cancel_token()
    }
}

crate::uniffi_repo_listeners!(InitRepoFfi, InitEvent);

#[uniffi::export]
impl InitRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx))]
    async fn load(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let (repo, stop_token) = fcx
            .do_on_rt(InitRepo::load(
                Arc::clone(&fcx.rcx.big_repo),
                fcx.rcx.doc_app.document_id(),
                fcx.rcx.local_user_path.clone().into(),
                fcx.rcx.sql.clone(),
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
