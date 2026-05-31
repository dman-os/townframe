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
        let repo = fcx
            .do_on_rt(daybook_core::blobs::BlobsRepo::new(
                fcx.rcx.layout.blobs_root.to_path_buf(),
                fcx.rcx.local_user_path.clone(),
                Arc::new(daybook_core::blobs::PartitionStoreMembershipWriter::new(
                    Arc::clone(&fcx.rcx.part_store),
                )),
            ))
            .await?;
        Ok(Arc::new(Self { fcx, repo }))
    }

    #[tracing::instrument(err, skip(self, data))]
    pub async fn put(&self, data: Vec<u8>) -> Result<String, FfiError> {
        let this = Arc::clone(&self.repo);
        self.fcx
            .do_on_rt(async move {
                this.put(&data, daybook_core::blobs::BlobUseHints::Unknown)
                    .await
                    .map(|blob_id| blob_id.to_string())
                    .map_err(FfiError::from)
            })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    pub async fn get_path(&self, hash: String) -> Result<String, FfiError> {
        let this = Arc::clone(&self.repo);
        self.fcx
            .do_on_rt(async move {
                let blob_id = hash
                    .parse::<daybook_core::blobs::BlobId>()
                    .wrap_err("error decoding blob hash")
                    .map_err(FfiError::from)?;
                this.get_path(blob_id)
                    .await
                    .map(|path| path.to_string_lossy().to_string())
                    .map_err(FfiError::from)
            })
            .await
    }
}
