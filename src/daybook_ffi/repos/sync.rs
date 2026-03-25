use crate::ffi::{CloneBootstrapInfo, CloneTicketWithQr, FfiError, SharedFfiCtx};
use crate::interlude::*;

use crate::repos::blobs::BlobsRepoFfi;
use crate::repos::config::ConfigRepoFfi;
use crate::repos::drawer::DrawerRepoFfi;
use crate::repos::progress::ProgressRepoFfi;

use daybook_core::index::{DocBlobsIndexRepo, DocBlobsIndexStopToken};
use daybook_core::local_state::SqliteLocalStateRepo;
use daybook_core::repos::RepoStopToken;
use daybook_core::sync::{IrohSyncRepo, IrohSyncRepoStopToken};
use qrcode::QrCode;

#[derive(uniffi::Object)]
pub struct SyncRepoFfi {
    fcx: SharedFfiCtx,
    pub repo: Arc<IrohSyncRepo>,
    sync_stop_token: tokio::sync::Mutex<Option<IrohSyncRepoStopToken>>,
    doc_blobs_index_stop_token: tokio::sync::Mutex<Option<DocBlobsIndexStopToken>>,
    sqlite_local_state_stop_token: tokio::sync::Mutex<Option<RepoStopToken>>,
}

fn bootstrap_to_ffi(bootstrap: daybook_core::sync::SyncBootstrapState) -> CloneBootstrapInfo {
    CloneBootstrapInfo {
        endpoint_id: bootstrap.endpoint_id.to_string(),
        repo_id: bootstrap.repo_id,
        repo_name: bootstrap.repo_name,
        app_doc_id: bootstrap.app_doc_id.to_string(),
        drawer_doc_id: bootstrap.drawer_doc_id.to_string(),
        device_name: bootstrap.device_name,
    }
}

#[uniffi::export]
impl SyncRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx, config_repo, blobs_repo, drawer_repo, progress_repo))]
    async fn load(
        fcx: SharedFfiCtx,
        config_repo: Arc<ConfigRepoFfi>,
        blobs_repo: Arc<BlobsRepoFfi>,
        drawer_repo: Arc<DrawerRepoFfi>,
        progress_repo: Arc<ProgressRepoFfi>,
    ) -> Result<Arc<Self>, FfiError> {
        let (sqlite_local_state_repo, sqlite_local_state_stop_token) = fcx
            .do_on_rt(SqliteLocalStateRepo::boot(
                fcx.rcx.layout.repo_root.join("local_state"),
            ))
            .await?;
        let (doc_blobs_index_repo, doc_blobs_index_stop_token) = fcx
            .do_on_rt(DocBlobsIndexRepo::boot(
                Arc::clone(&drawer_repo.repo),
                Arc::clone(&blobs_repo.repo),
                Arc::clone(&sqlite_local_state_repo),
            ))
            .await?;

        let (repo, sync_stop_token) = fcx
            .do_on_rt(IrohSyncRepo::boot(
                Arc::clone(&fcx.rcx),
                Arc::clone(&config_repo.repo),
                Arc::clone(&blobs_repo.repo),
                Arc::clone(&doc_blobs_index_repo),
                Some(Arc::clone(&progress_repo.repo)),
            ))
            .await?;

        Ok(Arc::new(Self {
            fcx,
            repo,
            sync_stop_token: Some(sync_stop_token).into(),
            doc_blobs_index_stop_token: Some(doc_blobs_index_stop_token).into(),
            sqlite_local_state_stop_token: Some(sqlite_local_state_stop_token).into(),
        }))
    }

    async fn stop(&self) -> Result<(), FfiError> {
        if let Some(token) = self.sync_stop_token.lock().await.take() {
            token.stop().await?;
        }
        if let Some(token) = self.doc_blobs_index_stop_token.lock().await.take() {
            token.stop().await?;
        }
        if let Some(token) = self.sqlite_local_state_stop_token.lock().await.take() {
            token.stop().await?;
        }
        Ok(())
    }

    async fn get_ticket_url(self: Arc<Self>) -> Result<String, FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move { this.repo.get_ticket_url().await.map_err(FfiError::from) })
            .await
    }

    async fn get_ticket_qr_png(self: Arc<Self>, size_px: u32) -> Result<Vec<u8>, FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move {
                let ticket_url = this.repo.get_ticket_url().await?;
                let qr_code = QrCode::new(ticket_url.as_bytes())
                    .map_err(|err| eyre::eyre!("failed to encode QR ticket: {err}"))?;
                let image = qr_code
                    .render::<image::Luma<u8>>()
                    .min_dimensions(size_px, size_px)
                    .build();
                let mut png_bytes = Vec::new();
                {
                    let mut cursor = std::io::Cursor::new(&mut png_bytes);
                    image::DynamicImage::ImageLuma8(image)
                        .write_to(&mut cursor, image::ImageFormat::Png)
                        .map_err(|err| eyre::eyre!("failed to render QR PNG bytes: {err}"))?;
                }
                Ok::<Vec<u8>, FfiError>(png_bytes)
            })
            .await
    }

    async fn get_ticket_with_qr_png(
        self: Arc<Self>,
        size_px: u32,
    ) -> Result<CloneTicketWithQr, FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move {
                let ticket_url = this.repo.get_ticket_url().await?;
                let qr_code = QrCode::new(ticket_url.as_bytes())
                    .map_err(|err| eyre::eyre!("failed to encode QR ticket: {err}"))?;
                let image = qr_code
                    .render::<image::Luma<u8>>()
                    .min_dimensions(size_px, size_px)
                    .build();
                let mut png_bytes = Vec::new();
                {
                    let mut cursor = std::io::Cursor::new(&mut png_bytes);
                    image::DynamicImage::ImageLuma8(image)
                        .write_to(&mut cursor, image::ImageFormat::Png)
                        .map_err(|err| eyre::eyre!("failed to render QR PNG bytes: {err}"))?;
                }
                Ok::<CloneTicketWithQr, FfiError>(CloneTicketWithQr {
                    ticket_url,
                    qr_png_bytes: png_bytes,
                })
            })
            .await
    }

    async fn connect_url(
        self: Arc<Self>,
        source_url: String,
    ) -> Result<CloneBootstrapInfo, FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move {
                let bootstrap = this.repo.connect_url(&source_url).await?;
                Ok::<CloneBootstrapInfo, FfiError>(bootstrap_to_ffi(bootstrap))
            })
            .await
    }

    async fn connect_known_devices_once(self: Arc<Self>) -> Result<(), FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move {
                this.repo
                    .connect_known_devices_once()
                    .await
                    .map_err(FfiError::from)
            })
            .await
    }
}
