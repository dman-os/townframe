use crate::interlude::*;
use crate::triage::TriageConfig;

#[derive(Reconcile, Hydrate, Default)]
pub struct ConfigStore {
    pub triage: TriageConfig,
    pub layout: LayoutWindowConfig,
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct LayoutWindowConfig {
    // pub tab_list_vis_expanded: Option<TabListVisibility>,
    // pub table_view_mode_compact: Option<TableViewMode>,
    // pub table_rail_vis_compact: Option<TabListVisibility>,
    // pub table_rail_vis_expanded: Option<TabListVisibility>,
    // pub sidebar_vis_expanded: Option<SidebarVisibility>,
    // pub sidebar_pos_expanded: Option<SidebarPosition>,
    // pub sidebar_mode_expanded: Option<SidebarMode>,
    // pub sidebar_auto_hide_expanded: Option<bool>,
    pub center_region: RootLayoutRegion,
    // pub bottom_region: RootLayoutRegion,
    pub left_region: RootLayoutRegion,
    pub right_region: RootLayoutRegion,
    // expanded specific settings
    pub left_visible: bool,
    pub right_visible: bool,
}

#[derive(Debug, Clone, Reconcile, Hydrate, Default)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum RegionSize {
    #[default]
    Auto,
    Weight, //(f32),
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct RootLayoutRegion {
    size: RegionSize,
    deets: LayoutPane,
}

impl Default for LayoutWindowConfig {
    fn default() -> Self {
        Self {
            center_region: RootLayoutRegion {
                size: default(),
                deets: LayoutPane {
                    key: "center".into(),
                    variant: LayoutPaneVariant::Routes(LayoutRoutes {}),
                },
            },
            left_region: RootLayoutRegion {
                size: default(),
                deets: LayoutPane {
                    key: "left".into(),
                    variant: LayoutPaneVariant::Sidebar(LayoutSidebar {}),
                },
            },
            right_region: RootLayoutRegion {
                size: default(),
                deets: LayoutPane {
                    key: "right".into(),
                    variant: LayoutPaneVariant::Region(LayoutRegion {
                        key: "right".into(),
                        orientation: Orientation::Vertical,
                        children: vec![
                            LayoutPane {
                                key: "top".into(),
                                variant: LayoutPaneVariant::Region(LayoutRegion {
                                    key: "top".into(),
                                    orientation: Orientation::Vertical,
                                    children: vec![],
                                }),
                            },
                            LayoutPane {
                                key: "bottom".into(),
                                variant: LayoutPaneVariant::Region(LayoutRegion {
                                    key: "bottom".into(),
                                    orientation: Orientation::Vertical,
                                    children: vec![],
                                }),
                            },
                        ],
                    }),
                },
            },
            left_visible: true,
            right_visible: false,
        }
    }
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct LayoutPane {
    key: String,
    variant: LayoutPaneVariant,
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum LayoutPaneVariant {
    // TODO: disallow this pane center region
    Sidebar(LayoutSidebar),
    Routes(LayoutRoutes),
    Region(LayoutRegion),
}

#[derive(Debug, Clone, Reconcile, Hydrate, Default)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct LayoutSidebar {}

#[derive(Debug, Clone, Reconcile, Hydrate, Default)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct LayoutRoutes {}

#[derive(Debug, Clone, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct LayoutRegion {
    key: String,
    orientation: Orientation,
    children: Vec<LayoutPane>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Reconcile, Hydrate, Default)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum Orientation {
    Horizontal,
    #[default]
    Vertical,
}

//

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
    pub async fn get_layout(&self) -> LayoutWindowConfig {
        self.store.query_sync(|store| store.layout.clone()).await
    }

    pub async fn set_layout(&self, value: LayoutWindowConfig) -> Res<()> {
        self.store
            .mutate_sync(move |store| {
                store.layout = value;
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
