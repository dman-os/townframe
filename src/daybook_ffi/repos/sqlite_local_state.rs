use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use daybook_core::local_state::{LocalStateEvent, SqliteLocalStateRepo};

#[derive(uniffi::Object)]
pub struct SqliteLocalStateRepoFfi {
    _fcx: SharedFfiCtx,
    pub repo: Arc<SqliteLocalStateRepo>,
    stop_token: tokio::sync::Mutex<Option<daybook_core::repos::RepoStopToken>>,
}

impl daybook_core::repos::Repo for SqliteLocalStateRepoFfi {
    type Event = LocalStateEvent;
    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.repo.registry
    }

    fn cancel_token(&self) -> &tokio_util::sync::CancellationToken {
        self.repo.cancel_token()
    }
}

crate::uniffi_repo_listeners!(SqliteLocalStateRepoFfi, LocalStateEvent);

#[uniffi::export]
impl SqliteLocalStateRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx))]
    async fn load(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let (repo, stop_token) = fcx
            .do_on_rt(SqliteLocalStateRepo::boot(
                fcx.rcx.layout.repo_root.join("local_state"),
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
