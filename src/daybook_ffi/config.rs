use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use daybook_core::config::{
    ConfigEvent, ConfigRepo, SidebarMode, SidebarPosition, SidebarVisibility, TabListVisibility, TableViewMode,
};

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

    // Tab list visibility settings
    #[tracing::instrument(skip(self))]
    async fn get_tab_list_vis_expanded(self: Arc<Self>) -> TabListVisibility {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.get_tab_list_vis_expanded().await })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    async fn set_tab_list_vis_expanded(
        self: Arc<Self>,
        value: TabListVisibility,
    ) -> Result<(), FfiError> {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.set_tab_list_vis_expanded(value).await })
            .await?;
        Ok(())
    }

    // Table view mode setting
    #[tracing::instrument(skip(self))]
    async fn get_table_view_mode_compact(self: Arc<Self>) -> TableViewMode {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.get_table_view_mode_compact().await })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    async fn set_table_view_mode_compact(
        self: Arc<Self>,
        value: TableViewMode,
    ) -> Result<(), FfiError> {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.set_table_view_mode_compact(value).await })
            .await?;
        Ok(())
    }

    // Table rail visibility settings
    #[tracing::instrument(skip(self))]
    async fn get_table_rail_vis_compact(self: Arc<Self>) -> TabListVisibility {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.get_table_rail_vis_compact().await })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    async fn set_table_rail_vis_compact(
        self: Arc<Self>,
        value: TabListVisibility,
    ) -> Result<(), FfiError> {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.set_table_rail_vis_compact(value).await })
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn get_table_rail_vis_expanded(self: Arc<Self>) -> TabListVisibility {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.get_table_rail_vis_expanded().await })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    async fn set_table_rail_vis_expanded(
        self: Arc<Self>,
        value: TabListVisibility,
    ) -> Result<(), FfiError> {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.set_table_rail_vis_expanded(value).await })
            .await?;
        Ok(())
    }

    // Sidebar visibility settings
    #[tracing::instrument(skip(self))]
    async fn get_sidebar_vis_expanded(self: Arc<Self>) -> SidebarVisibility {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.get_sidebar_vis_expanded().await })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    async fn set_sidebar_vis_expanded(
        self: Arc<Self>,
        value: SidebarVisibility,
    ) -> Result<(), FfiError> {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.set_sidebar_vis_expanded(value).await })
            .await?;
        Ok(())
    }

    // Sidebar position settings
    #[tracing::instrument(skip(self))]
    async fn get_sidebar_pos_expanded(self: Arc<Self>) -> SidebarPosition {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.get_sidebar_pos_expanded().await })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    async fn set_sidebar_pos_expanded(
        self: Arc<Self>,
        value: SidebarPosition,
    ) -> Result<(), FfiError> {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.set_sidebar_pos_expanded(value).await })
            .await?;
        Ok(())
    }

    // Sidebar mode settings
    #[tracing::instrument(skip(self))]
    async fn get_sidebar_mode_expanded(self: Arc<Self>) -> SidebarMode {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.get_sidebar_mode_expanded().await })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    async fn set_sidebar_mode_expanded(
        self: Arc<Self>,
        value: SidebarMode,
    ) -> Result<(), FfiError> {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.set_sidebar_mode_expanded(value).await })
            .await?;
        Ok(())
    }

    // Sidebar auto-hide settings
    #[tracing::instrument(skip(self))]
    async fn get_sidebar_auto_hide_expanded(self: Arc<Self>) -> bool {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.get_sidebar_auto_hide_expanded().await })
            .await
    }

    #[tracing::instrument(err, skip(self))]
    async fn set_sidebar_auto_hide_expanded(
        self: Arc<Self>,
        value: bool,
    ) -> Result<(), FfiError> {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.repo.set_sidebar_auto_hide_expanded(value).await })
            .await?;
        Ok(())
    }
}
