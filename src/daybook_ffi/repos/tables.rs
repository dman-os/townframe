use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use daybook_core::tables::{Panel, Tab, Table, TablesEvent, TablesPatches, TablesRepo, Window};

#[derive(uniffi::Object)]
struct TablesRepoFfi {
    fcx: SharedFfiCtx,
    repo: Arc<TablesRepo>,
    stop_token: tokio::sync::Mutex<Option<daybook_core::repos::RepoStopToken>>,
}

impl daybook_core::repos::Repo for TablesRepoFfi {
    type Event = TablesEvent;
    fn registry(&self) -> &Arc<daybook_core::repos::ListenersRegistry> {
        &self.repo.registry
    }

    fn cancel_token(&self) -> &tokio_util::sync::CancellationToken {
        self.repo.cancel_token()
    }
}

crate::uniffi_repo_listeners!(TablesRepoFfi, TablesEvent);

#[uniffi::export]
impl TablesRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx))]
    async fn load(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let (repo, stop_token) = fcx
            .do_on_rt(TablesRepo::load(
                fcx.rcx.acx.clone(),
                fcx.rcx.doc_app.document_id().clone(),
                fcx.rcx.local_actor_id.clone(),
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
    async fn get_window(self: Arc<Self>, id: Uuid) -> Option<Window> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move { this.repo.get_window(id).await })
            .await
    }

    #[tracing::instrument(err, skip(self, window))]
    async fn set_window(
        self: Arc<Self>,
        id: Uuid,
        window: Window,
    ) -> Result<Option<Window>, FfiError> {
        let this = Arc::clone(&self);
        let out = self
            .fcx
            .do_on_rt(async move { this.repo.set_window(id, window).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn list_windows(self: Arc<Self>) -> Result<Vec<Window>, FfiError> {
        let this = Arc::clone(&self);
        let out = self
            .fcx
            .do_on_rt(async move { this.repo.list_windows().await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn get_tab(self: Arc<Self>, id: Uuid) -> Result<Option<Tab>, FfiError> {
        let this = Arc::clone(&self);
        let out = self
            .fcx
            .do_on_rt(async move { this.repo.get_tab(id).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self, tab))]
    async fn set_tab(self: Arc<Self>, id: Uuid, tab: Tab) -> Result<Option<Tab>, FfiError> {
        let this = Arc::clone(&self);
        let out = self
            .fcx
            .do_on_rt(async move { this.repo.set_tab(id, tab).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn list_tabs(self: Arc<Self>) -> Result<Vec<Tab>, FfiError> {
        let this = Arc::clone(&self);
        let out = self
            .fcx
            .do_on_rt(async move { this.repo.list_tab().await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(skip(self))]
    async fn get_panel(self: Arc<Self>, id: Uuid) -> Option<Panel> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move { this.repo.get_panel(id).await })
            .await
    }

    #[tracing::instrument(err, skip(self, panel))]
    async fn set_panel(self: Arc<Self>, id: Uuid, panel: Panel) -> Result<Option<Panel>, FfiError> {
        let this = Arc::clone(&self);
        let out = self
            .fcx
            .do_on_rt(async move { this.repo.set_panel(id, panel).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn list_panels(self: Arc<Self>) -> Result<Vec<Panel>, FfiError> {
        let this = Arc::clone(&self);
        let out = self
            .fcx
            .do_on_rt(async move { this.repo.list_panel().await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn get_table(self: Arc<Self>, id: Uuid) -> Result<Option<Table>, FfiError> {
        let this = Arc::clone(&self);
        let out = self
            .fcx
            .do_on_rt(async move { this.repo.get_table(id).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self, table))]
    async fn set_table(self: Arc<Self>, id: Uuid, table: Table) -> Result<Option<Table>, FfiError> {
        let this = Arc::clone(&self);
        let out = self
            .fcx
            .do_on_rt(async move { this.repo.set_table(id, table).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn list_tables(self: Arc<Self>) -> Result<Vec<Table>, FfiError> {
        let this = Arc::clone(&self);
        let out = self
            .fcx
            .do_on_rt(async move { this.repo.list_tables().await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn get_selected_table(self: Arc<Self>) -> Result<Option<Table>, FfiError> {
        let this = Arc::clone(&self);
        let out = self
            .fcx
            .do_on_rt(async move { this.repo.get_selected_table().await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self, patches))]
    async fn update_batch(self: Arc<Self>, patches: TablesPatches) -> Result<(), FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move { this.repo.update_batch(patches).await })
            .await?;
        Ok(())
    }

    #[tracing::instrument(err, skip(self))]
    async fn create_new_table(self: Arc<Self>) -> Result<Uuid, FfiError> {
        let this = Arc::clone(&self);
        let out = self
            .fcx
            .do_on_rt(async move { this.repo.create_new_table().await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn create_new_tab(self: Arc<Self>, table_id: Uuid) -> Result<Uuid, FfiError> {
        let this = Arc::clone(&self);
        let out = self
            .fcx
            .do_on_rt(async move { this.repo.create_new_tab(table_id).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn remove_tab(self: Arc<Self>, tab_id: Uuid) -> Result<(), FfiError> {
        let this = Arc::clone(&self);
        self.fcx
            .do_on_rt(async move { this.repo.remove_tab(tab_id).await })
            .await?;
        Ok(())
    }
}
