use crate::interlude::*;
use crate::triage::TriageConfig;

#[derive(Reconcile, Hydrate, Default)]
pub struct ConfigStore {
    pub triage: TriageConfig,
    pub tab_list_vis_expanded: Option<TabListVisibility>,
    pub table_view_mode_compact: Option<TableViewMode>,
    pub table_rail_vis_compact: Option<TabListVisibility>,
    pub table_rail_vis_expanded: Option<TabListVisibility>,
    pub sidebar_vis_expanded: Option<SidebarVisibility>,
    pub sidebar_pos_expanded: Option<SidebarPosition>,
    pub sidebar_mode_expanded: Option<SidebarMode>,
    pub sidebar_auto_hide_expanded: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Reconcile, Hydrate, Default)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum TabListVisibility {
    Visible,
    #[default]
    Hidden,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Reconcile, Hydrate, Default)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum TableViewMode {
    #[default]
    Hidden,
    Rail,
    TabRow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Reconcile, Hydrate, Default)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum SidebarVisibility {
    #[default]
    Visible,
    Hidden,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Reconcile, Hydrate, Default)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum SidebarPosition {
    Left,
    #[default]
    Right,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Reconcile, Hydrate, Default)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum SidebarMode {
    #[default]
    Hidden,
    Compact,
    Expanded,
}

impl ConfigStore {
    pub const PROP: &str = "config";

    pub async fn load(acx: &AmCtx, app_doc_id: &DocumentId) -> Res<Self> {
        Ok(acx
            .hydrate_path::<Self>(app_doc_id, automerge::ROOT, vec![Self::PROP.into()])
            .await?
            .unwrap_or_default())
    }

    /// Register a change listener for config changes
    pub async fn register_change_listener<F>(
        acx: &AmCtx,
        broker: &utils_rs::am::changes::DocChangeBroker,
        on_change: F,
    ) -> Res<()>
    where
        F: Fn(Vec<utils_rs::am::changes::ChangeNotification>) + Send + Sync + 'static,
    {
        acx.change_manager()
            .add_listener(
                utils_rs::am::changes::ChangeFilter {
                    path: vec![Self::PROP.into()],
                    doc_id: Some(broker.filter()),
                },
                on_change,
            )
            .await;
        Ok(())
    }
}

#[async_trait]
impl crate::stores::Store for ConfigStore {
    type FlushArgs = (AmCtx, DocumentId);

    async fn flush(&mut self, (acx, app_doc_id): &mut Self::FlushArgs) -> Res<()> {
        acx.reconcile_prop(app_doc_id, automerge::ROOT, Self::PROP, self)
            .await
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum ConfigEvent {
    Changed,
}

pub struct ConfigRepo {
    acx: AmCtx,
    app_doc_id: DocumentId,
    store: crate::stores::StoreHandle<ConfigStore>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    broker: Arc<utils_rs::am::changes::DocChangeBroker>,
}

impl crate::repos::Repo for ConfigRepo {
    type Event = ConfigEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }
}

impl ConfigRepo {
    pub async fn load(acx: AmCtx, app_doc_id: DocumentId) -> Res<Arc<Self>> {
        let registry = crate::repos::ListenersRegistry::new();

        let store = ConfigStore::load(&acx, &app_doc_id).await?;
        let store = crate::stores::StoreHandle::new(store, (acx.clone(), app_doc_id.clone()));

        let broker = {
            let handle = acx
                .find_doc(&app_doc_id)
                .await?
                .expect("doc should have been loaded");
            acx.change_manager().add_doc(handle)
        };

        let (notif_tx, notif_rx) = tokio::sync::mpsc::unbounded_channel::<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >();
        // Register change listener to automatically notify repo listeners
        ConfigStore::register_change_listener(&acx, &broker, {
            move |notifs| notif_tx.send(notifs).expect(ERROR_CHANNEL)
        })
        .await?;

        let repo = Self {
            acx: acx.clone(),
            app_doc_id: app_doc_id.clone(),
            store,
            registry: registry.clone(),
            broker,
        };
        let repo = Arc::new(repo);

        let _notif_worker = tokio::spawn({
            let repo = repo.clone();
            async move { repo.handle_notifs(notif_rx).await }
        });

        Ok(repo)
    }

    async fn handle_notifs(
        self: &Self,
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >,
    ) -> Res<()> {
        while let Some(_notifs) = notif_rx.recv().await {
            // Config changed, notify listeners
            self.registry.notify(ConfigEvent::Changed);
        }
        Ok(())
    }

    pub async fn get_triage_config_sync(&self) -> TriageConfig {
        self.store.query_sync(|store| store.triage.clone()).await
    }

    pub async fn get_config_heads(&self) -> Res<Arc<[automerge::ChangeHash]>> {
        let handle = self
            .acx
            .find_doc(&self.app_doc_id)
            .await?
            .ok_or_eyre("app doc not found")?;
        let heads = handle.with_document(|doc| doc.get_heads());
        Ok(Arc::from(heads))
    }

    pub async fn add_processor(
        &self,
        processor_id: String,
        processor: crate::triage::Processor,
    ) -> Res<()> {
        self.store
            .mutate_sync(move |store| {
                store.triage.processors.insert(processor_id, processor);
            })
            .await?;
        Ok(())
    }

    // Tab list visibility settings
    pub async fn get_tab_list_vis_expanded(&self) -> TabListVisibility {
        self.store
            .query_sync(|store| store.tab_list_vis_expanded.unwrap_or_default())
            .await
    }

    pub async fn set_tab_list_vis_expanded(&self, value: TabListVisibility) -> Res<()> {
        info!(?value, "set_tab_list_vis_expanded XXX");
        self.store
            .mutate_sync(move |store| {
                store.tab_list_vis_expanded = Some(value);
            })
            .await?;
        Ok(())
    }

    // Table view mode setting
    pub async fn get_table_view_mode_compact(&self) -> TableViewMode {
        self.store
            .query_sync(|store| store.table_view_mode_compact.unwrap_or_default())
            .await
    }

    pub async fn set_table_view_mode_compact(&self, value: TableViewMode) -> Res<()> {
        self.store
            .mutate_sync(move |store| {
                store.table_view_mode_compact = Some(value);
            })
            .await?;
        Ok(())
    }

    // Table rail visibility settings
    pub async fn get_table_rail_vis_compact(&self) -> TabListVisibility {
        self.store
            .query_sync(|store| store.table_rail_vis_compact.unwrap_or_default())
            .await
    }

    pub async fn set_table_rail_vis_compact(&self, value: TabListVisibility) -> Res<()> {
        self.store
            .mutate_sync(move |store| {
                store.table_rail_vis_compact = Some(value);
            })
            .await?;
        Ok(())
    }

    pub async fn get_table_rail_vis_expanded(&self) -> TabListVisibility {
        self.store
            .query_sync(|store| store.table_rail_vis_expanded.unwrap_or_default())
            .await
    }

    pub async fn set_table_rail_vis_expanded(&self, value: TabListVisibility) -> Res<()> {
        info!(?value, "XXX");
        self.store
            .mutate_sync(move |store| {
                store.table_rail_vis_expanded = Some(value);
            })
            .await?;
        Ok(())
    }

    // Sidebar visibility settings
    pub async fn get_sidebar_vis_expanded(&self) -> SidebarVisibility {
        self.store
            .query_sync(|store| store.sidebar_vis_expanded.unwrap_or_default())
            .await
    }

    pub async fn set_sidebar_vis_expanded(&self, value: SidebarVisibility) -> Res<()> {
        self.store
            .mutate_sync(move |store| {
                store.sidebar_vis_expanded = Some(value);
            })
            .await?;
        Ok(())
    }

    // Sidebar position settings
    pub async fn get_sidebar_pos_expanded(&self) -> SidebarPosition {
        self.store
            .query_sync(|store| store.sidebar_pos_expanded.unwrap_or_default())
            .await
    }

    pub async fn set_sidebar_pos_expanded(&self, value: SidebarPosition) -> Res<()> {
        self.store
            .mutate_sync(move |store| {
                store.sidebar_pos_expanded = Some(value);
            })
            .await?;
        Ok(())
    }

    // Sidebar mode settings
    pub async fn get_sidebar_mode_expanded(&self) -> SidebarMode {
        self.store
            .query_sync(|store| store.sidebar_mode_expanded.unwrap_or_default())
            .await
    }

    pub async fn set_sidebar_mode_expanded(&self, value: SidebarMode) -> Res<()> {
        self.store
            .mutate_sync(move |store| {
                store.sidebar_mode_expanded = Some(value);
            })
            .await?;
        Ok(())
    }

    // Sidebar auto-hide settings
    pub async fn get_sidebar_auto_hide_expanded(&self) -> bool {
        self.store
            .query_sync(|store| store.sidebar_auto_hide_expanded.unwrap_or(false))
            .await
    }

    pub async fn set_sidebar_auto_hide_expanded(&self, value: bool) -> Res<()> {
        self.store
            .mutate_sync(move |store| {
                store.sidebar_auto_hide_expanded = Some(value);
            })
            .await?;
        Ok(())
    }
}

pub mod version_updates {
    use crate::interlude::*;

    use automerge::{transaction::Transactable, ActorId, AutoCommit, ROOT};
    use autosurgeon::reconcile_prop;

    pub fn version_latest() -> Res<Vec<u8>> {
        let mut doc = AutoCommit::new().with_actor(ActorId::random());
        doc.put(ROOT, "version", "0")?;
        // indicate schema type for this document
        doc.put(ROOT, "$schema", "daybook.config")?;
        reconcile_prop(
            &mut doc,
            ROOT,
            super::ConfigStore::PROP,
            super::ConfigStore::default(),
        )?;
        Ok(doc.save_nocompress())
    }
}
