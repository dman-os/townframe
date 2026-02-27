use crate::ffi::{FfiError, SharedFfiCtx};
use crate::interlude::*;

use daybook_core::progress::{
    CreateProgressTaskArgs, ProgressEvent, ProgressRepo, ProgressRetentionPolicy, ProgressTask,
    ProgressUpdate, ProgressUpdateEntry,
};

#[derive(uniffi::Object)]
pub struct ProgressRepoFfi {
    fcx: SharedFfiCtx,
    pub repo: Arc<ProgressRepo>,
}

impl daybook_core::repos::Repo for ProgressRepoFfi {
    type Event = ProgressEvent;

    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.repo.registry
    }

    fn cancel_token(&self) -> &tokio_util::sync::CancellationToken {
        self.repo.cancel_token()
    }
}

crate::uniffi_repo_listeners!(ProgressRepoFfi, ProgressEvent);

#[uniffi::export]
impl ProgressRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx))]
    pub async fn load(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let repo = fcx
            .do_on_rt(ProgressRepo::boot(fcx.rcx.sql.db_pool.clone()))
            .await
            .inspect_err(|err| tracing::error!(?err))?;
        Ok(Arc::new(Self { fcx, repo }))
    }

    pub async fn upsert_task(
        self: Arc<Self>,
        args: CreateProgressTaskArgs,
    ) -> Result<(), FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move { this.repo.upsert_task(args).await.map_err(FfiError::from) })
            .await
    }

    pub async fn add_update(
        self: Arc<Self>,
        task_id: String,
        update: ProgressUpdate,
    ) -> Result<(), FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move {
                this.repo
                    .add_update(&task_id, update)
                    .await
                    .map_err(FfiError::from)
            })
            .await
    }

    pub async fn mark_viewed(self: Arc<Self>, task_id: String) -> Result<(), FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move {
                this.repo
                    .mark_viewed(&task_id)
                    .await
                    .map_err(FfiError::from)
            })
            .await
    }

    pub async fn dismiss(self: Arc<Self>, task_id: String) -> Result<(), FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move { this.repo.dismiss(&task_id).await.map_err(FfiError::from) })
            .await
    }

    pub async fn set_retention_override(
        self: Arc<Self>,
        task_id: String,
        retention_override: Option<ProgressRetentionPolicy>,
    ) -> Result<(), FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move {
                this.repo
                    .set_retention_override(&task_id, retention_override)
                    .await
                    .map_err(FfiError::from)
            })
            .await
    }

    pub async fn clear_completed(self: Arc<Self>) -> Result<u64, FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move { this.repo.clear_completed().await.map_err(FfiError::from) })
            .await
    }

    pub async fn get(self: Arc<Self>, task_id: String) -> Result<Option<ProgressTask>, FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move { this.repo.get(&task_id).await.map_err(FfiError::from) })
            .await
    }

    pub async fn list(self: Arc<Self>) -> Result<Vec<ProgressTask>, FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move { this.repo.list().await.map_err(FfiError::from) })
            .await
    }

    pub async fn list_by_tag_prefix(
        self: Arc<Self>,
        tag_prefix: String,
    ) -> Result<Vec<ProgressTask>, FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move {
                this.repo
                    .list_by_tag_prefix(&tag_prefix)
                    .await
                    .map_err(FfiError::from)
            })
            .await
    }

    pub async fn list_updates(
        self: Arc<Self>,
        task_id: String,
    ) -> Result<Vec<ProgressUpdateEntry>, FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move {
                this.repo
                    .list_updates(&task_id)
                    .await
                    .map_err(FfiError::from)
            })
            .await
    }
}
