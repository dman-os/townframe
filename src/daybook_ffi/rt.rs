use crate::ffi::{FfiError, SharedFfiCtx};
use crate::interlude::*;

#[derive(uniffi::Object)]
pub struct RtFfi {
    fcx: SharedFfiCtx,
    stop_token: tokio::sync::Mutex<Option<daybook_core::rt::RtStopToken>>,
    pub rt: Arc<daybook_core::rt::Rt>,
}

#[uniffi::export]
impl RtFfi {
    #[uniffi::constructor]
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        err,
        skip(
            fcx,
            drawer_repo,
            plugs_repo,
            dispatch_repo,
            progress_repo,
            blobs_repo,
            config_repo
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
        device_id: String,
    ) -> Result<Arc<Self>, FfiError> {
        let cx = Arc::clone(fcx.repo_ctx());
        let repo_root = cx.repo_root().to_path_buf();
        let wflow_db_url = format!("sqlite:{}?mode=rwc", repo_root.join("wflow.db").display());
        let local_state_root = repo_root.join("local_states");

        let (rt, stop_token) = fcx
            .do_on_rt(daybook_core::rt::Rt::boot(
                daybook_core::rt::RtConfig { device_id },
                cx.doc_app().document_id().clone(),
                wflow_db_url,
                cx.acx().clone(),
                Arc::clone(&drawer_repo.repo),
                Arc::clone(&plugs_repo.repo),
                Arc::clone(&dispatch_repo.repo),
                Arc::clone(&progress_repo.repo),
                Arc::clone(&blobs_repo.repo),
                Arc::clone(&config_repo.repo),
                cx.local_actor_id().clone(),
                local_state_root,
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
        if let Some(token) = self.stop_token.lock().await.take() {
            token.stop().await?;
        }
        Ok(())
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
                    daybook_core::rt::DispatchArgs::DocFacet {
                        doc_id,
                        branch_path: daybook_types::doc::BranchPath::from(branch_path),
                        heads: daybook_types::doc::ChangeHashSet(vec![].into()),
                        facet_key: None,
                    },
                )
                .await
            })
            .await?;
        Ok(dispatch_id)
    }
}
