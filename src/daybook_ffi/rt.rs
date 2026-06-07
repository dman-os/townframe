use crate::ffi::{FfiError, SharedFfiCtx};
use crate::interlude::*;

#[derive(uniffi::Record)]
pub struct RenderedFacetViewRecord {
    pub plug_id: String,
    pub view_key: String,
    pub view: daybook_types::view::ViewSpec,
    pub plugin_state_json: Option<String>,
}

#[derive(uniffi::Object)]
pub struct RtFfi {
    fcx: SharedFfiCtx,
    stop_token: tokio::sync::Mutex<Option<daybook_core::rt::RtStopToken>>,
    pub rt: Arc<daybook_core::rt::Rt>,
}

#[uniffi::export]
impl RtFfi {
    #[uniffi::constructor]
    #[expect(clippy::too_many_arguments)]
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
    pub async fn load(
        fcx: SharedFfiCtx,
        drawer_repo: Arc<crate::repos::drawer::DrawerRepoFfi>,
        plugs_repo: Arc<crate::repos::plugs::PlugsRepoFfi>,
        dispatch_repo: Arc<crate::repos::dispatch::DispatchRepoFfi>,
        progress_repo: Arc<crate::repos::progress::ProgressRepoFfi>,
        blobs_repo: Arc<crate::repos::blobs::BlobsRepoFfi>,
        config_repo: Arc<crate::repos::config::ConfigRepoFfi>,
        init_repo: Arc<crate::repos::init::InitRepoFfi>,
        sqlite_ls_repo: Arc<crate::repos::sqlite_local_state::SqliteLocalStateRepoFfi>,
        device_id: String,
        startup_progress_task_id: Option<String>,
    ) -> Result<Arc<Self>, FfiError> {
        plugs_repo.repo.ensure_system_plugs().await?;

        let (rt, stop_token) = fcx
            .do_on_rt(daybook_core::rt::Rt::boot(
                daybook_core::rt::RtConfig {
                    device_id,
                    startup_progress_task_id,
                },
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
            stop_token: Some(stop_token).into(),
            rt,
        }))
    }

    pub async fn stop(&self) -> Result<(), FfiError> {
        let stop_token = self.stop_token.lock().await.take();
        self.fcx
            .do_on_rt(async move {
                if let Some(token) = stop_token {
                    token.stop().await?;
                }
                Ok::<(), FfiError>(())
            })
            .await
    }

    pub async fn dispatch_doc_facet(
        self: Arc<Self>,
        plug_id: String,
        routine_name: String,
        doc_id: String,
        branch_path: String,
    ) -> Result<String, FfiError> {
        let rt = Arc::clone(&self.rt);
        let dispatch_id = self
            .fcx
            .do_on_rt(async move {
                rt.dispatch(
                    &plug_id,
                    &routine_name,
                    daybook_core::rt::DispatchArgs::DocRoutine {
                        doc_id,
                        branch_path: daybook_types::doc::BranchPathBuf::from(branch_path),
                        heads: daybook_types::doc::ChangeHashSet(vec![].into()),
                        invocation: daybook_core::rt::dispatch::RoutineInvocation::Command,
                        changed_facet_keys: vec![],
                        wflow_args_json: None,
                    },
                )
                .await
        })
        .await?;
        Ok(dispatch_id)
    }

    pub async fn render_facet_view(
        self: Arc<Self>,
        doc_id: String,
        branch_path: String,
        facet_key: String,
        requested_view: Option<daybook_types::manifest::ViewRef>,
        ui_state_json: Option<String>,
    ) -> Result<RenderedFacetViewRecord, FfiError> {
        let fcx = Arc::clone(&self.fcx);
        let rt = Arc::clone(&self.rt);
        let branch_path = daybook_types::doc::BranchPathBuf::from(branch_path);
        let facet_key = daybook_types::doc::FacetKey::from(facet_key);
        let view = fcx
            .do_on_rt(async move {
                rt
                    .render_facet_view(
                        &doc_id,
                        &branch_path,
                        &facet_key,
                        requested_view,
                        ui_state_json,
                    )
                    .await
            })
            .await?;

        let view_spec = serde_json::from_str::<daybook_types::view::ViewSpec>(&view.view_json)
            .wrap_err("runtime returned invalid ViewSpec JSON")?;

        Ok(RenderedFacetViewRecord {
            plug_id: view.plug_id,
            view_key: view.view_key,
            view: view_spec,
            plugin_state_json: view.plugin_state_json,
        })
    }
}
