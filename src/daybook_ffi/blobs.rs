use crate::interlude::*;
use crate::ffi::{FfiError, SharedFfiCtx};

use daybook_core::blobs::BlobsRepo;

#[derive(uniffi::Object)]
pub struct BlobsRepoFfi {
    fcx: SharedFfiCtx,
    repo: Arc<BlobsRepo>,
}

#[uniffi::export]
impl BlobsRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx))]
    pub async fn load(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let repo = Arc::new(fcx.cx.blobs.clone());
        Ok(Arc::new(Self { fcx, repo }))
    }

    #[tracing::instrument(err, skip(self, data))]
    pub async fn put(&self, data: Vec<u8>) -> Result<String, FfiError> {
        let this = self.repo.clone();
        Ok(self.fcx.do_on_rt(async move {
            this.put(&data).await.map_err(FfiError::from)
        }).await?)
    }

    #[tracing::instrument(err, skip(self))]
    pub async fn get_path(&self, hash: String) -> Result<String, FfiError> {
        let this = self.repo.clone();
        Ok(self.fcx.do_on_rt(async move {
            this.get_path(&hash).await
                .map(|p| p.to_string_lossy().to_string())
                .map_err(FfiError::from)
        }).await?)
    }
}

impl BlobsRepoFfi {
    pub fn new(fcx: SharedFfiCtx) -> Arc<Self> {
        let repo = Arc::new(fcx.cx.blobs.clone());
        Arc::new(Self { fcx, repo })
    }
}
