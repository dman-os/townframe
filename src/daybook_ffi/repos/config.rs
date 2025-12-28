use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use crate::repos::plugs::PlugsRepoFfi;
use daybook_core::config::{ConfigEvent, ConfigRepo};
use daybook_core::plugs::manifest::PropKeyDisplayHint;

#[derive(uniffi::Record)]
pub struct PropKeyDisplayHintEntry {
    pub key: String,
    pub config: PropKeyDisplayHint,
}

#[derive(uniffi::Object)]
struct ConfigRepoFfi {
    fcx: SharedFfiCtx,
    repo: Arc<ConfigRepo>,
}

impl daybook_core::repos::Repo for ConfigRepoFfi {
    type Event = ConfigEvent;
    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.repo.registry
    }
}

crate::uniffi_repo_listeners!(ConfigRepoFfi, ConfigEvent);

#[uniffi::export]
impl ConfigRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx, plug_repo))]
    async fn load(fcx: SharedFfiCtx, plug_repo: Arc<PlugsRepoFfi>) -> Result<Arc<Self>, FfiError> {
        let fcx = fcx.clone();
        let repo = fcx
            .do_on_rt(ConfigRepo::load(
                fcx.cx.acx.clone(),
                fcx.cx.doc_app().document_id().clone(),
                plug_repo.repo.clone(),
            ))
            .await
            .inspect_err(|err| tracing::error!(?err))?;
        Ok(Arc::new(Self { fcx, repo }))
    }

    #[tracing::instrument(skip(self))]
    async fn get_prop_display_hint(&self, id: String) -> Option<PropKeyDisplayHint> {
        let repo = self.repo.clone();
        self.fcx
            .do_on_rt(async move { repo.get_prop_display_hint(id).await })
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn list_display_hints(self: Arc<Self>) -> HashMap<String, PropKeyDisplayHint> {
        let repo = self.repo.clone();
        self.fcx
            .do_on_rt(async move { repo.list_display_hints().await })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    async fn set_meta_table_key_config(
        &self,
        key: String,
        config: PropKeyDisplayHint,
    ) -> Result<(), FfiError> {
        let repo = self.repo.clone();
        self.fcx
            .do_on_rt(async move {
                repo.set_prop_display_hint(key, config)
                    .await
                    .map_err(FfiError::from)
            })
            .await
    }
}
