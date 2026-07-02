use crate::ffi::{FfiError, SharedFfiCtx};
use crate::interlude::*;

use crate::repos::blobs::BlobsRepoFfi;
use crate::repos::config::ConfigRepoFfi;
use crate::repos::dispatch::DispatchRepoFfi;
use crate::repos::drawer::DrawerRepoFfi;
use crate::repos::init::InitRepoFfi;
use crate::repos::plugs::PlugsRepoFfi;
use crate::repos::progress::ProgressRepoFfi;
use crate::repos::sqlite_local_state::SqliteLocalStateRepoFfi;

use daybook_core::rt::{Rt, RtConfig, RtStopToken};
use daybook_types::manifest::ViewRef;
use daybook_types::view::ViewSpec;

#[derive(Debug, Clone, uniffi::Record)]
pub struct RenderedFacetView {
    pub view: ViewSpec,
    pub plugin_state_json: Option<String>,
}

#[derive(uniffi::Object)]
pub struct RtFfi {
    fcx: SharedFfiCtx,
    pub rt: Arc<Rt>,
    stop_token: tokio::sync::Mutex<Option<RtStopToken>>,
    _drawer_repo: Arc<DrawerRepoFfi>,
    _plugs_repo: Arc<PlugsRepoFfi>,
    _dispatch_repo: Arc<DispatchRepoFfi>,
    _progress_repo: Arc<ProgressRepoFfi>,
    _blobs_repo: Arc<BlobsRepoFfi>,
    _config_repo: Arc<ConfigRepoFfi>,
    _init_repo: Arc<InitRepoFfi>,
    _sqlite_ls_repo: Arc<SqliteLocalStateRepoFfi>,
}

#[uniffi::export]
impl RtFfi {
    #[uniffi::constructor]
    #[tracing::instrument(
        err,
        skip(
            fcx,
            drawer_repo,
            plugs_repo,
            dispatch_repo,
            progress_repo,
            blobs_repo,
            config_repo,
            init_repo,
            sqlite_ls_repo,
        )
    )]
    #[expect(clippy::too_many_arguments)]
    async fn load(
        fcx: SharedFfiCtx,
        drawer_repo: Arc<DrawerRepoFfi>,
        plugs_repo: Arc<PlugsRepoFfi>,
        dispatch_repo: Arc<DispatchRepoFfi>,
        progress_repo: Arc<ProgressRepoFfi>,
        blobs_repo: Arc<BlobsRepoFfi>,
        config_repo: Arc<ConfigRepoFfi>,
        init_repo: Arc<InitRepoFfi>,
        sqlite_ls_repo: Arc<SqliteLocalStateRepoFfi>,
        device_id: String,
        startup_progress_task_id: Option<String>,
    ) -> Result<Arc<Self>, FfiError> {
        let rt_config = RtConfig {
            device_id,
            startup_progress_task_id,
        };
        let (rt, stop_token) = fcx
            .do_on_rt(Rt::boot(
                rt_config,
                Arc::clone(&fcx.rcx),
                Arc::clone(&drawer_repo.repo),
                Arc::clone(&plugs_repo.repo),
                Arc::clone(&dispatch_repo.repo),
                Arc::clone(&progress_repo.repo),
                Arc::clone(&blobs_repo.repo),
                Arc::clone(&config_repo.repo),
                Arc::clone(&init_repo.repo),
                Arc::clone(&sqlite_ls_repo.repo),
            ))
            .await
            .inspect_err(|err| tracing::error!(?err))?;

        Ok(Arc::new(Self {
            fcx,
            rt,
            stop_token: Some(stop_token).into(),
            _drawer_repo: drawer_repo,
            _plugs_repo: plugs_repo,
            _dispatch_repo: dispatch_repo,
            _progress_repo: progress_repo,
            _blobs_repo: blobs_repo,
            _config_repo: config_repo,
            _init_repo: init_repo,
            _sqlite_ls_repo: sqlite_ls_repo,
        }))
    }

    async fn stop(&self) -> Result<(), FfiError> {
        let stop_token = self.stop_token.lock().await.take();
        self.fcx
            .do_on_rt(async move {
                if let Some(token) = stop_token {
                    token.stop().await.map_err(FfiError::from)?;
                }
                Ok::<(), FfiError>(())
            })
            .await
    }

    async fn render_facet_view(
        &self,
        doc_id: String,
        branch_path: String,
        facet_key: String,
        requested_view: Option<ViewRef>,
        ui_state_json: Option<String>,
    ) -> Result<RenderedFacetView, FfiError> {
        let this = Arc::clone(&self.rt);
        let branch_path = daybook_types::doc::BranchPathBuf::from(branch_path);
        let facet_key = daybook_types::doc::FacetKey::from(facet_key);
        let rendered = self
            .fcx
            .do_on_rt(async move {
                this.render_facet_view(
                    &doc_id,
                    &branch_path,
                    &facet_key,
                    requested_view,
                    ui_state_json,
                )
                .await
            })
            .await
            .map_err(FfiError::from)?;
        let view = serde_json::from_str::<ViewSpec>(&rendered.view_json)
            .map_err(eyre::Report::from)
            .map_err(FfiError::from)?;
        Ok(RenderedFacetView {
            view,
            plugin_state_json: rendered.plugin_state_json,
        })
    }
}
