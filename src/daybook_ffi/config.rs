use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use daybook_core::config::{ConfigEvent, ConfigRepo, MetaTableKeyConfig};

#[derive(uniffi::Record)]
pub struct MetaTableKeyConfigEntry {
    pub key: String,
    pub config: MetaTableKeyConfig,
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
    #[tracing::instrument(err, skip(fcx))]
    async fn load(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let fcx = fcx.clone();
        let repo = fcx
            .do_on_rt(ConfigRepo::load(
                fcx.cx.acx.clone(),
                fcx.cx.doc_app().document_id().clone(),
            ))
            .await
            .inspect_err(|err| tracing::error!(?err))?;
        Ok(Arc::new(Self { fcx, repo }))
    }

    #[tracing::instrument(err, skip(self))]
    async fn get_meta_table_key_configs(&self) -> Result<Vec<MetaTableKeyConfigEntry>, FfiError> {
        let repo = self.repo.clone();
        let configs = self
            .fcx
            .do_on_rt(async move { repo.get_meta_table_key_configs_sync().await })
            .await;
        Ok(configs
            .into_iter()
            .map(|(k, v)| MetaTableKeyConfigEntry { key: k, config: v })
            .collect())
    }

    #[tracing::instrument(err, skip(self))]
    async fn get_meta_table_key_config(
        &self,
        key: String,
    ) -> Result<Option<daybook_core::config::MetaTableKeyConfig>, FfiError> {
        let repo = self.repo.clone();
        Ok(self
            .fcx
            .do_on_rt(async move { repo.get_meta_table_key_config_sync(key).await })
            .await)
    }

    #[tracing::instrument(err, skip(self))]
    async fn set_meta_table_key_config(
        &self,
        key: String,
        config: daybook_core::config::MetaTableKeyConfig,
    ) -> Result<(), FfiError> {
        let repo = self.repo.clone();
        self.fcx
            .do_on_rt(async move {
                repo.set_meta_table_key_config(key, config)
                    .await
                    .map_err(FfiError::from)
            })
            .await
    }
}
