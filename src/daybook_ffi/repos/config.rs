use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use crate::repos::plugs::PlugsRepoFfi;
use daybook_core::config::{ConfigEvent, ConfigRepo};
use daybook_core::plugs::manifest::FacetDisplayHint;

#[derive(uniffi::Record)]
pub struct FacetKeyDisplayHintEntry {
    pub key: String,
    pub config: FacetDisplayHint,
}

#[derive(uniffi::Object)]
struct ConfigRepoFfi {
    fcx: SharedFfiCtx,
    repo: Arc<ConfigRepo>,
    stop_token: tokio::sync::Mutex<Option<daybook_core::repos::RepoStopToken>>,
}

impl daybook_core::repos::Repo for ConfigRepoFfi {
    type Event = ConfigEvent;
    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.repo.registry
    }

    fn cancel_token(&self) -> &tokio_util::sync::CancellationToken {
        self.repo.cancel_token()
    }
}

crate::uniffi_repo_listeners!(ConfigRepoFfi, ConfigEvent);

#[uniffi::export]
impl ConfigRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx, plug_repo))]
    async fn load(fcx: SharedFfiCtx, plug_repo: Arc<PlugsRepoFfi>) -> Result<Arc<Self>, FfiError> {
        let fcx = Arc::clone(&fcx);
        let (repo, stop_token) = fcx
            .do_on_rt(ConfigRepo::load(
                fcx.cx.acx.clone(),
                fcx.cx.doc_app().document_id().clone(),
                Arc::clone(&plug_repo.repo),
                daybook_types::doc::UserPath::from(fcx.cx.local_user_path.clone()),
            ))
            .await
            .inspect_err(|err| tracing::error!(?err))?;
        Ok(Arc::new(Self {
            fcx,
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

    #[tracing::instrument(skip(self))]
    async fn get_facet_display_hint(&self, id: String) -> Option<FacetDisplayHint> {
        let repo = Arc::clone(&self.repo);
        self.fcx
            .do_on_rt(async move { repo.get_facet_display_hint(id).await })
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn list_display_hints(self: Arc<Self>) -> HashMap<String, FacetDisplayHint> {
        let repo = Arc::clone(&self.repo);
        self.fcx
            .do_on_rt(async move { repo.list_display_hints().await })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    async fn set_facet_display_hint(
        &self,
        key: String,
        config: FacetDisplayHint,
    ) -> Result<(), FfiError> {
        let repo = Arc::clone(&self.repo);
        self.fcx
            .do_on_rt(async move {
                repo.set_facet_display_hint(key, config)
                    .await
                    .map_err(FfiError::from)
            })
            .await
    }
}
