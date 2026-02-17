use crate::ffi::{FfiError, SharedFfiCtx};
use crate::interlude::*;

use daybook_core::blobs::BlobsRepo;

#[derive(uniffi::Object)]
pub struct BlobsRepoFfi {
    fcx: SharedFfiCtx,
    pub repo: Arc<BlobsRepo>,
}

#[uniffi::export]
impl BlobsRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx))]
    pub async fn load(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let cx = Arc::clone(fcx.repo_ctx());
        let repo = fcx
            .do_on_rt(daybook_core::blobs::BlobsRepo::new(
                cx.blobs_root().to_path_buf(),
            ))
            .await?;
        Ok(Arc::new(Self { fcx, repo }))
    }

    #[tracing::instrument(err, skip(self, data))]
    pub async fn put(&self, data: Vec<u8>) -> Result<String, FfiError> {
        let this = Arc::clone(&self.repo);
        self.fcx
            .do_on_rt(async move { this.put(&data).await.map_err(FfiError::from) })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    pub async fn get_path(&self, hash: String) -> Result<String, FfiError> {
        let this = Arc::clone(&self.repo);
        self.fcx
            .do_on_rt(async move {
                this.get_path(&hash)
                    .await
                    .map(|path| path.to_string_lossy().to_string())
                    .map_err(FfiError::from)
            })
            .await
    }
}
