use crate::interlude::*;
use tokio_util::sync::CancellationToken;

/// Constants for sidebar layout weights
mod sidebar_layout {
    /// Default expanded sidebar weight (40% of available space)
    pub const DEFAULT_SIDEBAR_WEIGHT: f32 = 0.4;

    /// Default weight for documents screen list size when expanded
    pub const DOCUMENTS_LIST_EXPANDED_WEIGHT: f32 = 0.4;
}

#[derive(Debug, Clone, Reconcile, Hydrate, Patch, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[patch(attribute(derive(Debug, Default)))]
#[cfg_attr(feature = "uniffi", patch(attribute(derive(uniffi::Record))))]
pub struct Window {
    #[key]
    pub id: Uuid,
    pub title: String,
    pub tabs: Vec<Uuid>,
    pub selected_table: Option<Uuid>,
    pub layout: WindowLayout,
    pub last_capture_mode: CaptureMode,
    pub documents_screen_list_size_expanded: WindowLayoutRegionSize,
}

#[derive(Debug, Clone, Copy, Reconcile, Hydrate, Default, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum CaptureMode {
    #[default]
    Text,
    Camera,
    Mic,
}

#[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct WindowLayout {
    pub center_region: WindowLayoutRegionChild,
    pub left_region: WindowLayoutRegionChild,
    pub right_region: WindowLayoutRegionChild,
    pub left_visible: bool,
    pub right_visible: bool,
}

#[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum WindowLayoutRegionSize {
    Weight(f32),
}

impl Default for WindowLayoutRegionSize {
    fn default() -> Self {
        Self::Weight(1.0)
    }
}

#[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct WindowLayoutRegionChild {
    pub size: WindowLayoutRegionSize,
    pub deets: WindowLayoutPane,
}

impl Default for WindowLayout {
    fn default() -> Self {
        Self {
            center_region: WindowLayoutRegionChild {
                size: WindowLayoutRegionSize::Weight(1.0),
                deets: WindowLayoutPane {
                    key: "center".into(),
                    variant: WindowLayoutPaneVariant::Routes(WindowLayoutRoutes {}),
                },
            },
            left_region: WindowLayoutRegionChild {
                size: WindowLayoutRegionSize::Weight(sidebar_layout::DEFAULT_SIDEBAR_WEIGHT),
                deets: WindowLayoutPane {
                    key: "left".into(),
                    variant: WindowLayoutPaneVariant::Sidebar(WindowLayoutSidebar {}),
                },
            },
            right_region: WindowLayoutRegionChild {
                size: WindowLayoutRegionSize::Weight(sidebar_layout::DEFAULT_SIDEBAR_WEIGHT),
                deets: WindowLayoutPane {
                    key: "right".into(),
                    variant: WindowLayoutPaneVariant::Region(WindowLayoutRegion {
                        key: "right".into(),
                        orientation: WindowLayoutOrientation::Vertical,
                        children: vec![
                            WindowLayoutRegionChild {
                                size: WindowLayoutRegionSize::Weight(0.5),
                                deets: WindowLayoutPane {
                                    key: "top".into(),
                                    variant: WindowLayoutPaneVariant::Region(WindowLayoutRegion {
                                        key: "top".into(),
                                        orientation: WindowLayoutOrientation::Vertical,
                                        children: vec![],
                                    }),
                                },
                            },
                            WindowLayoutRegionChild {
                                size: WindowLayoutRegionSize::Weight(0.5),
                                deets: WindowLayoutPane {
                                    key: "bottom".into(),
                                    variant: WindowLayoutPaneVariant::Region(WindowLayoutRegion {
                                        key: "bottom".into(),
                                        orientation: WindowLayoutOrientation::Vertical,
                                        children: vec![],
                                    }),
                                },
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

#[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct WindowLayoutPane {
    pub key: String,
    pub variant: WindowLayoutPaneVariant,
}

#[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum WindowLayoutPaneVariant {
    Sidebar(WindowLayoutSidebar),
    Routes(WindowLayoutRoutes),
    Region(WindowLayoutRegion),
}

#[derive(Debug, Clone, Reconcile, Hydrate, Default, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct WindowLayoutSidebar {}

#[derive(Debug, Clone, Reconcile, Hydrate, Default, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct WindowLayoutRoutes {}

#[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct WindowLayoutRegion {
    pub key: String,
    pub orientation: WindowLayoutOrientation,
    pub children: Vec<WindowLayoutRegionChild>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Reconcile, Hydrate, Default)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum WindowLayoutOrientation {
    Horizontal,
    #[default]
    Vertical,
}

#[derive(Debug, Clone, Reconcile, Hydrate, PartialEq, Patch)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[patch(attribute(derive(Debug, Default)))]
#[cfg_attr(feature = "uniffi", patch(attribute(derive(uniffi::Record))))]
pub struct Table {
    #[key]
    pub id: Uuid,
    pub title: String,
    pub tabs: Vec<Uuid>,
    pub window: TableWindow,
    pub selected_tab: Option<Uuid>,
}

#[derive(Debug, Clone, Reconcile, Hydrate, Default, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum TableWindow {
    #[default]
    AllWindows,
    Specific {
        id: Uuid,
    },
}

#[derive(Debug, Clone, Reconcile, Hydrate, Patch, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[patch(attribute(derive(Debug, Default)))]
#[cfg_attr(feature = "uniffi", patch(attribute(derive(uniffi::Record))))]
pub struct Tab {
    #[key]
    pub id: Uuid,
    pub title: String,
    pub panels: Vec<Uuid>,
    pub selected_panel: Option<Uuid>,
}

#[derive(Debug, Clone, Reconcile, Hydrate, Patch, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[patch(attribute(derive(Debug, Default)))]
#[cfg_attr(feature = "uniffi", patch(attribute(derive(uniffi::Record))))]
pub struct Panel {
    #[key]
    pub id: Uuid,
    pub title: String,
}

#[derive(Debug)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct TablesPatches {
    pub tab_updates: Option<Vec<TabPatch>>,
    pub window_updates: Option<Vec<WindowPatch>>,
    pub panel_updates: Option<Vec<PanelPatch>>,
    pub table_updates: Option<Vec<TablePatch>>,
}

// FIXME: store leaf types in Arcs and
// just use new Arcs on update. (No mutexes)
#[derive(Reconcile, Hydrate, Default)]
pub struct TablesStore {
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    pub windows: HashMap<Uuid, Window>,
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    pub tables: HashMap<Uuid, Table>,
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    pub tabs: HashMap<Uuid, Tab>,
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    pub panels: HashMap<Uuid, Panel>,

    // Indices for tracking relationships (not stored in CRDT)
    #[autosurgeon(with = "utils_rs::am::codecs::skip")]
    pub panel_to_tab: HashMap<Uuid, Uuid>, // panel_id -> tab_id
    #[autosurgeon(with = "utils_rs::am::codecs::skip")]
    pub tab_to_table: HashMap<Uuid, Uuid>, // tab_id -> table_id
    #[autosurgeon(with = "utils_rs::am::codecs::skip")]
    pub tab_to_window: HashMap<Uuid, Uuid>, // tab_id -> window_id
}

#[async_trait]
impl crate::stores::Store for TablesStore {
    const PROP: &str = "tables";
}

impl TablesStore {
    // Auto-create a default table with window, tab, and panel
    fn auto_create_default_all(&mut self) {
        let window_id = Uuid::new_v4();
        let table_id = Uuid::new_v4();
        let tab_id = Uuid::new_v4();
        let panel_id = Uuid::new_v4();

        // Create window
        let window = Window {
            id: window_id,
            title: "Main Window".to_string(),
            tabs: vec![tab_id],
            selected_table: Some(table_id),
            layout: WindowLayout::default(),
            last_capture_mode: CaptureMode::default(),
            documents_screen_list_size_expanded: WindowLayoutRegionSize::Weight(
                sidebar_layout::DOCUMENTS_LIST_EXPANDED_WEIGHT,
            ),
        };
        self.windows.insert(window_id, window);

        // Create table
        let table = Table {
            id: table_id,
            title: "Main Table".to_string(),
            tabs: vec![tab_id],
            window: TableWindow::Specific { id: window_id },
            selected_tab: Some(tab_id),
        };
        self.tables.insert(table_id, table);

        // Create tab
        let tab = Tab {
            id: tab_id,
            title: "Main Tab".to_string(),
            panels: vec![panel_id],
            selected_panel: Some(panel_id),
        };
        self.tabs.insert(tab_id, tab);

        // Create panel
        let panel = Panel {
            id: panel_id,
            title: "Main Panel".to_string(),
        };
        self.panels.insert(panel_id, panel);

        // Update indices
        self.tab_to_table.insert(tab_id, table_id);
        self.tab_to_window.insert(tab_id, window_id);
        self.panel_to_tab.insert(panel_id, tab_id);
    }
}

impl TablesStore {
    // Rebuild all indices from scratch
    pub fn rebuild_indices(&mut self) {
        self.panel_to_tab.clear();
        self.tab_to_table.clear();
        self.tab_to_window.clear();

        // Build panel -> tab index
        for (tab_id, tab) in &self.tabs {
            for panel_id in &tab.panels {
                self.panel_to_tab.insert(*panel_id, *tab_id);
            }
        }

        // Build tab -> table index
        for (table_id, table) in &self.tables {
            for tab_id in &table.tabs {
                self.tab_to_table.insert(*tab_id, *table_id);
            }
        }

        // Build tab -> window index
        for (window_id, window) in &self.windows {
            for tab_id in &window.tabs {
                self.tab_to_window.insert(*tab_id, *window_id);
            }
        }
    }

    // Update indices when a panel is added/removed from a tab
    pub fn update_panel_tab_index(
        &mut self,
        panel_id: Uuid,
        old_tab_id: Option<Uuid>,
        new_tab_id: Option<Uuid>,
    ) {
        if let Some(_old_tab) = old_tab_id {
            self.panel_to_tab.remove(&panel_id);
        }
        if let Some(new_tab) = new_tab_id {
            self.panel_to_tab.insert(panel_id, new_tab);
        }
    }

    // Update indices when a tab is added/removed from a table
    pub fn update_tab_table_index(
        &mut self,
        tab_id: Uuid,
        old_table_id: Option<Uuid>,
        new_table_id: Option<Uuid>,
    ) {
        if let Some(_old_table) = old_table_id {
            self.tab_to_table.remove(&tab_id);
        }
        if let Some(new_table) = new_table_id {
            self.tab_to_table.insert(tab_id, new_table);
        }
    }

    // Update indices when a tab is added/removed from a window
    pub fn update_tab_window_index(
        &mut self,
        tab_id: Uuid,
        old_window_id: Option<Uuid>,
        new_window_id: Option<Uuid>,
    ) {
        if let Some(_old_window) = old_window_id {
            self.tab_to_window.remove(&tab_id);
        }
        if let Some(new_window) = new_window_id {
            self.tab_to_window.insert(tab_id, new_window);
        }
    }
}

pub struct TablesRepo {
    store: crate::stores::StoreHandle<TablesStore>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
    _change_listener_tickets: Vec<utils_rs::am::changes::ChangeListenerRegistration>,
}

impl crate::repos::Repo for TablesRepo {
    type Event = TablesEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }
    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

// Granular event enum for specific changes
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum TablesEvent {
    ListChanged,
    WindowChanged { id: Uuid },
    TabChanged { id: Uuid },
    PanelChanged { id: Uuid },
    TableChanged { id: Uuid },
}

impl TablesRepo {
    pub async fn load(
        acx: AmCtx,
        app_doc_id: DocumentId,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let registry = crate::repos::ListenersRegistry::new();

        let store = TablesStore::load(&acx, &app_doc_id).await?;
        let store = crate::stores::StoreHandle::new(store, acx.clone(), app_doc_id.clone());
        store
            .mutate_sync(|store| {
                store.rebuild_indices();
            })
            .await?;

        let (broker, broker_stop) = {
            let handle = acx
                .find_doc(&app_doc_id)
                .await?
                .expect("doc should have been loaded");
            acx.change_manager().add_doc(handle).await?
        };

        let (notif_tx, notif_rx) = tokio::sync::mpsc::unbounded_channel::<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >();
        // Register change listener to automatically notify repo listeners
        let ticket = TablesStore::register_change_listener(&acx, &broker, vec![], {
            move |notifs| {
                if let Err(err) = notif_tx.send(notifs) {
                    warn!("failed to send change notifications: {err}");
                }
            }
        })
        .await?;

        let main_cancel_token = CancellationToken::new();
        let repo = Self {
            store,
            registry: registry.clone(),
            cancel_token: main_cancel_token.child_token(),
            _change_listener_tickets: vec![ticket],
        };
        let repo = Arc::new(repo);

        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let cancel_token = main_cancel_token.clone();
            async move {
                repo.handle_notifs(notif_rx, cancel_token)
                    .await
                    .expect("error handling notifs")
            }
        });

        Ok((
            repo,
            crate::repos::RepoStopToken {
                cancel_token: main_cancel_token,
                worker_handle: Some(worker_handle),
                broker_stop_tokens: broker_stop.into_iter().collect(),
            },
        ))
    }

    async fn handle_notifs(
        self: &Self,
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        // FIXME: this code doesn't seem right and has missing features
        let mut events = vec![];
        loop {
            let notifs = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    while let Ok(notifs) = notif_rx.try_recv() {
                        self.process_notifs(notifs, &mut events).await?;
                    }
                    break;
                }
                msg = notif_rx.recv() => {
                    match msg {
                        Some(notifs) => notifs,
                        None => break,
                    }
                }
            };
            self.process_notifs(notifs, &mut events).await?;
        }
        Ok(())
    }

    async fn process_notifs(
        &self,
        notifs: Vec<utils_rs::am::changes::ChangeNotification>,
        events: &mut Vec<TablesEvent>,
    ) -> Res<()> {
        events.clear();
        for notif in notifs {
            match &notif.patch.action {
                automerge::PatchAction::PutMap { key, .. } => {
                    // Check if this is a specific item change
                    if let Ok(uuid) = Uuid::parse_str(key) {
                        // Determine which type of item changed based on path
                        if notif.patch.path.len() >= 2 {
                            match &notif.patch.path[1].1 {
                                automerge::Prop::Map(path_key) => match path_key.as_ref() {
                                    "windows" => {
                                        events.push(TablesEvent::WindowChanged { id: uuid })
                                    }
                                    "tables" => events.push(TablesEvent::TableChanged { id: uuid }),
                                    "tabs" => events.push(TablesEvent::TabChanged { id: uuid }),
                                    "panels" => events.push(TablesEvent::PanelChanged { id: uuid }),
                                    _ => events.push(TablesEvent::ListChanged),
                                },
                                _ => events.push(TablesEvent::ListChanged),
                            }
                        } else {
                            events.push(TablesEvent::ListChanged);
                        }
                    }
                }
                automerge::PatchAction::DeleteMap { key } => {
                    // Handle deletions
                    if let Ok(uuid) = Uuid::parse_str(key) {
                        if notif.patch.path.len() >= 2 {
                            match &notif.patch.path[1].1 {
                                automerge::Prop::Map(path_key) => match path_key.as_ref() {
                                    "windows" => {
                                        events.push(TablesEvent::WindowChanged { id: uuid })
                                    }
                                    "tables" => events.push(TablesEvent::TableChanged { id: uuid }),
                                    "tabs" => events.push(TablesEvent::TabChanged { id: uuid }),
                                    "panels" => events.push(TablesEvent::PanelChanged { id: uuid }),
                                    _ => events.push(TablesEvent::ListChanged),
                                },
                                _ => events.push(TablesEvent::ListChanged),
                            }
                        } else {
                            events.push(TablesEvent::ListChanged);
                        }
                    }
                }
                _ => {
                    // For other operations, send ListChanged
                }
            }
        }
        for evt in events.drain(..) {
            self.registry.notify([evt]);
        }
        Ok(())
    }

    // end impl TablesRepo

    // Helper method to find which tab contains a panel using index
    async fn find_tab_for_panel(&self, panel_id: Uuid) -> Option<Uuid> {
        self.store
            .query_sync(|store| store.panel_to_tab.get(&panel_id).copied())
            .await
    }

    // Helper method to find which table contains a tab using index
    async fn find_table_for_tab(&self, tab_id: Uuid) -> Option<Uuid> {
        self.store
            .query_sync(|store| store.tab_to_table.get(&tab_id).copied())
            .await
    }

    // Helper method to find which window contains a tab using index
    async fn find_window_for_tab(&self, tab_id: Uuid) -> Option<Uuid> {
        self.store
            .query_sync(|store| store.tab_to_window.get(&tab_id).copied())
            .await
    }

    pub async fn get_window(&self, id: Uuid) -> Option<Window> {
        self.store
            .query_sync(|store| store.windows.get(&id).cloned())
            .await
    }

    pub async fn set_window(&self, id: Uuid, val: Window) -> Res<Option<Window>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        self.store
            .mutate_sync(|store| {
                let old_window = store.windows.get(&id).cloned();
                let old_tabs = old_window
                    .as_ref()
                    .map(|w| w.tabs.clone())
                    .unwrap_or_default();

                for tab_id in &old_tabs {
                    if !val.tabs.contains(tab_id) {
                        store.tab_to_window.remove(tab_id);
                    }
                }
                for tab_id in &val.tabs {
                    if !old_tabs.contains(tab_id) {
                        store.tab_to_window.insert(*tab_id, id);
                    }
                }

                let old = store.windows.insert(id, val);

                self.registry
                    .notify([TablesEvent::WindowChanged { id }, TablesEvent::ListChanged]);
                old
            })
            .await
            .map(|(res, _)| res)
    }

    pub async fn list_windows(&self) -> Res<Vec<Window>> {
        let out = self
            .store
            .query_sync(|store| store.windows.values().cloned().collect())
            .await;
        Ok(out)
    }

    pub async fn get_tab(&self, id: Uuid) -> Res<Option<Tab>> {
        let out = self
            .store
            .query_sync(|store| store.tabs.get(&id).cloned())
            .await;
        Ok(out)
    }

    pub async fn set_tab(&self, id: Uuid, val: Tab) -> Res<Option<Tab>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let (old, _) = self
            .store
            .mutate_sync(|store| {
                // Get old tab to check for panel changes
                let old_tab = store.tabs.get(&id).cloned();
                let old_panels = old_tab
                    .as_ref()
                    .map(|t| t.panels.clone())
                    .unwrap_or_default();

                // Update panel-to-tab index for changed panels
                for panel_id in &old_panels {
                    if !val.panels.contains(panel_id) {
                        store.panel_to_tab.remove(panel_id);
                    }
                }
                for panel_id in &val.panels {
                    if !old_panels.contains(panel_id) {
                        store.panel_to_tab.insert(*panel_id, id);
                    }
                }
                store.tabs.insert(id, val)
            })
            .await?;

        // Send cascading events using indices (read from indices)
        let mut notifs = vec![TablesEvent::TabChanged { id }];
        if let Some(table_id) = self.find_table_for_tab(id).await {
            notifs.push(TablesEvent::TableChanged { id: table_id });
        }
        if let Some(window_id) = self.find_window_for_tab(id).await {
            notifs.push(TablesEvent::WindowChanged { id: window_id });
        }
        notifs.push(TablesEvent::ListChanged);
        self.registry.notify(notifs);
        Ok(old)
    }

    pub async fn list_tab(&self) -> Res<Vec<Tab>> {
        let out = self
            .store
            .query_sync(|store| store.tabs.values().cloned().collect())
            .await;
        Ok(out)
    }

    pub async fn get_table(&self, id: Uuid) -> Res<Option<Table>> {
        let out = self
            .store
            .query_sync(|store| store.tables.get(&id).cloned())
            .await;
        Ok(out)
    }

    pub async fn set_table(&self, id: Uuid, val: Table) -> Res<Option<Table>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let (old, _) = self
            .store
            .mutate_sync(|store| {
                // Get old table to check for tab changes
                let old_table = store.tables.get(&id).cloned();
                let old_tabs = old_table
                    .as_ref()
                    .map(|t| t.tabs.clone())
                    .unwrap_or_default();

                // Update tab-to-table index for changed tabs
                for tab_id in &old_tabs {
                    if !val.tabs.contains(tab_id) {
                        store.tab_to_table.remove(tab_id);
                    }
                }
                for tab_id in &val.tabs {
                    if !old_tabs.contains(tab_id) {
                        store.tab_to_table.insert(*tab_id, id);
                    }
                }

                // Auto-create tab if all tabs were removed
                if val.tabs.is_empty() {
                    // create a new tab and panel inside the same mutation
                    let tab_id = Uuid::new_v4();
                    let panel_id = Uuid::new_v4();
                    let tab = Tab {
                        id: tab_id,
                        title: "New Tab".to_string(),
                        panels: vec![panel_id],
                        selected_panel: Some(panel_id),
                    };
                    store.tabs.insert(tab_id, tab);
                    let panel = Panel {
                        id: panel_id,
                        title: "New Panel".to_string(),
                    };
                    store.panels.insert(panel_id, panel);
                    if let Some(table) = store.tables.get_mut(&id) {
                        table.tabs.push(tab_id);
                        table.selected_tab = Some(tab_id);
                    }
                }
                store.tables.insert(id, val)
            })
            .await?;

        self.registry
            .notify([TablesEvent::TableChanged { id }, TablesEvent::ListChanged]);
        Ok(old)
    }

    pub async fn list_tables(&self) -> Res<Vec<Table>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let (tables, _) = self
            .store
            .mutate_sync(|store| {
                let tables: Vec<Table> = store.tables.values().cloned().collect();
                if tables.is_empty() {
                    store.auto_create_default_all();
                    store.tables.values().cloned().collect()
                } else {
                    tables
                }
            })
            .await?;

        Ok(tables)
    }

    pub async fn get_panel(&self, id: Uuid) -> Option<Panel> {
        self.store
            .query_sync(|store| store.panels.get(&id).cloned())
            .await
    }

    pub async fn set_panel(&self, id: Uuid, val: Panel) -> Res<Option<Panel>> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let (old, _) = self
            .store
            .mutate_sync(|store| store.panels.insert(id, val))
            .await?;

        // Send cascading events using indices (read from indices)
        self.registry.notify([TablesEvent::PanelChanged { id }]);
        if let Some(tab_id) = self.find_tab_for_panel(id).await {
            self.registry
                .notify([TablesEvent::TabChanged { id: tab_id }]);
            if let Some(table_id) = self.find_table_for_tab(tab_id).await {
                self.registry
                    .notify([TablesEvent::TableChanged { id: table_id }]);
            }
            if let Some(window_id) = self.find_window_for_tab(tab_id).await {
                self.registry
                    .notify([TablesEvent::WindowChanged { id: window_id }]);
            }
        }
        self.registry.notify([TablesEvent::ListChanged]);
        Ok(old)
    }

    pub async fn list_panel(&self) -> Res<Vec<Panel>> {
        let out = self
            .store
            .query_sync(|store| store.panels.values().cloned().collect())
            .await;
        Ok(out)
    }

    pub async fn update_batch(&self, patches: TablesPatches) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        // Move patches into the closure to avoid cloning non-cloneable patch types
        self.store
            .mutate_sync(move |store| {
                // Apply tab updates
                if let Some(tab_updates) = patches.tab_updates {
                    for tab_patch in tab_updates {
                        if let Some(id) = tab_patch.id {
                            if let Some(tab) = store.tabs.get_mut(&id) {
                                tab.apply(tab_patch);
                            }
                        }
                    }
                }

                // Apply window updates
                if let Some(window_updates) = patches.window_updates {
                    for window_patch in window_updates {
                        if let Some(id) = window_patch.id {
                            if let Some(window) = store.windows.get_mut(&id) {
                                window.apply(window_patch);
                            }
                        }
                    }
                }

                // Apply panel updates
                if let Some(panel_updates) = patches.panel_updates {
                    for panel_patch in panel_updates {
                        if let Some(id) = panel_patch.id {
                            if let Some(panel) = store.panels.get_mut(&id) {
                                panel.apply(panel_patch);
                            }
                        }
                    }
                }

                // Apply table updates
                if let Some(table_updates) = patches.table_updates {
                    for table_patch in table_updates {
                        if let Some(id) = table_patch.id {
                            if let Some(table) = store.tables.get_mut(&id) {
                                table.apply(table_patch);
                            }
                        }
                    }
                }

                // Rebuild indices after all updates
                store.rebuild_indices();
            })
            .await?;

        self.registry.notify([TablesEvent::ListChanged]);
        Ok(())
    }

    // Get the selected table from the first window
    pub async fn get_selected_table(&self) -> Res<Option<Table>> {
        let out = self
            .store
            .query_sync(|store| {
                // Find the first window with a selected table
                for window in store.windows.values() {
                    if let Some(selected_table_id) = window.selected_table {
                        if let Some(table) = store.tables.get(&selected_table_id) {
                            return Some(table.clone());
                        }
                    }
                }

                // If no selected table found, return the first table
                store.tables.iter().next().map(|(_, t)| t.clone())
            })
            .await;
        Ok(out)
    }

    // Create a new table with a default tab and panel
    pub async fn create_new_table(&self) -> Res<Uuid> {
        let (table_id, _) = self
            .store
            .mutate_sync(|store| {
                let table_id = Uuid::new_v4();
                let tab_id = Uuid::new_v4();
                let panel_id = Uuid::new_v4();

                // Find or create a window for this table
                let window_id = if let Some((&id, _)) = store.windows.iter().next() {
                    id
                } else {
                    // Create a new window
                    let new_window_id = Uuid::new_v4();
                    let window = Window {
                        id: new_window_id,
                        title: "Main Window".to_string(),
                        tabs: vec![tab_id],
                        selected_table: Some(table_id),
                        layout: WindowLayout::default(),
                        last_capture_mode: CaptureMode::default(),
                        documents_screen_list_size_expanded: WindowLayoutRegionSize::Weight(
                            sidebar_layout::DOCUMENTS_LIST_EXPANDED_WEIGHT,
                        ),
                    };
                    store.windows.insert(new_window_id, window);
                    new_window_id
                };

                // Create table
                let table = Table {
                    id: table_id,
                    title: format!("Table {}", store.tables.len() + 1),
                    tabs: vec![tab_id],
                    window: TableWindow::Specific { id: window_id },
                    selected_tab: Some(tab_id),
                };
                store.tables.insert(table_id, table);

                // Create tab
                let tab = Tab {
                    id: tab_id,
                    title: format!("Tab {}", store.tabs.len() + 1),
                    panels: vec![panel_id],
                    selected_panel: Some(panel_id),
                };
                store.tabs.insert(tab_id, tab);

                // Create panel
                let panel = Panel {
                    id: panel_id,
                    title: format!("Panel {}", store.panels.len() + 1),
                };
                store.panels.insert(panel_id, panel);

                // Update window to include the new tab
                if let Some(window) = store.windows.get_mut(&window_id) {
                    if !window.tabs.contains(&tab_id) {
                        window.tabs.push(tab_id);
                    }
                    window.selected_table = Some(table_id);
                }

                // Update indices
                store.tab_to_table.insert(tab_id, table_id);
                store.tab_to_window.insert(tab_id, window_id);
                store.panel_to_tab.insert(panel_id, tab_id);

                table_id
            })
            .await?;

        self.registry
            .notify([TablesEvent::TableChanged { id: table_id }]);
        self.registry.notify([TablesEvent::ListChanged]);

        Ok(table_id)
    }

    // Create a new tab for an existing table
    pub async fn create_new_tab(&self, table_id: Uuid) -> Res<Uuid> {
        // Single lock: use try_mutate so we can return errors inside the closure
        let (tab_id, _) = self
            .store
            .try_mutate_sync(move |store| {
                // Get the table window policy (owned) to avoid borrowing across await
                let table_window = store
                    .tables
                    .get(&table_id)
                    .map(|t| t.window.clone())
                    .ok_or_eyre("Table not found")?;
                let window_id = match table_window {
                    TableWindow::Specific { id } => id,
                    TableWindow::AllWindows => {
                        if let Some((&id, _)) = store.windows.iter().next() {
                            id
                        } else {
                            let new_window_id = Uuid::new_v4();
                            let window = Window {
                                id: new_window_id,
                                title: "Main Window".to_string(),
                                tabs: vec![],
                                selected_table: Some(table_id),
                                layout: WindowLayout::default(),
                                last_capture_mode: CaptureMode::default(),
                                documents_screen_list_size_expanded: WindowLayoutRegionSize::Weight(
                                    sidebar_layout::DOCUMENTS_LIST_EXPANDED_WEIGHT,
                                ),
                            };
                            store.windows.insert(new_window_id, window);
                            new_window_id
                        }
                    }
                };

                let tab_id = Uuid::new_v4();
                let panel_id = Uuid::new_v4();

                // Create tab and panel
                let tab = Tab {
                    id: tab_id,
                    title: format!("Tab {}", store.tabs.len() + 1),
                    panels: vec![panel_id],
                    selected_panel: Some(panel_id),
                };
                store.tabs.insert(tab_id, tab);
                let panel = Panel {
                    id: panel_id,
                    title: format!("Panel {}", store.panels.len() + 1),
                };
                store.panels.insert(panel_id, panel);

                // Update table and window
                if let Some(table) = store.tables.get_mut(&table_id) {
                    table.tabs.push(tab_id);
                    table.selected_tab = Some(tab_id);
                }
                if let Some(window) = store.windows.get_mut(&window_id) {
                    if !window.tabs.contains(&tab_id) {
                        window.tabs.push(tab_id);
                    }
                }

                // Update indices
                store.tab_to_table.insert(tab_id, table_id);
                store.tab_to_window.insert(tab_id, window_id);
                store.panel_to_tab.insert(panel_id, tab_id);

                Ok(tab_id)
            })
            .await?;

        self.registry
            .notify([TablesEvent::TableChanged { id: table_id }]);
        self.registry.notify([TablesEvent::ListChanged]);

        Ok(tab_id)
    }

    // Remove a tab and its panel
    pub async fn remove_tab(&self, tab_id: Uuid) -> Res<()> {
        // Single lock: read and mutate inside try_mutate to avoid double-lock
        let (table_id, _) = self
            .store
            .try_mutate_sync(|store| {
                // Read needed values owned (avoid borrowing across await)
                let panel_ids = store
                    .tabs
                    .get(&tab_id)
                    .map(|t| t.panels.clone())
                    .ok_or_eyre("Tab not found")?;
                let table_id = store.tab_to_table.get(&tab_id).copied();
                let window_id = store.tab_to_window.get(&tab_id).copied();

                // Remove the tab
                store.tabs.remove(&tab_id);

                // Remove all panels in this tab
                for panel_id in panel_ids {
                    store.panels.remove(&panel_id);
                    store.panel_to_tab.remove(&panel_id);
                }

                // Update table to remove the tab
                if let Some(table_id) = table_id {
                    if let Some(table) = store.tables.get_mut(&table_id) {
                        table.tabs.retain(|&id| id != tab_id);
                        // If this was the selected tab, select another one or clear selection
                        if table.selected_tab == Some(tab_id) {
                            table.selected_tab = table.tabs.first().copied();
                        }

                        // Auto-create a new tab if this was the last tab
                        if table.tabs.is_empty() {
                            let new_tab_id = Uuid::new_v4();
                            let new_panel_id = Uuid::new_v4();
                            let tab = Tab {
                                id: new_tab_id,
                                title: "New Tab".to_string(),
                                panels: vec![new_panel_id],
                                selected_panel: Some(new_panel_id),
                            };
                            store.tabs.insert(new_tab_id, tab);
                            let panel = Panel {
                                id: new_panel_id,
                                title: "New Panel".to_string(),
                            };
                            store.panels.insert(new_panel_id, panel);
                            table.tabs.push(new_tab_id);
                            table.selected_tab = Some(new_tab_id);
                            // update indices for new tab
                            store.tab_to_table.insert(new_tab_id, table_id);
                            if let Some(window_id) = window_id {
                                store.tab_to_window.insert(new_tab_id, window_id);
                                if let Some(window) = store.windows.get_mut(&window_id) {
                                    if !window.tabs.contains(&new_tab_id) {
                                        window.tabs.push(new_tab_id);
                                    }
                                }
                            }
                        }
                    }
                }

                // Update window to remove the tab
                if let Some(window_id) = window_id {
                    if let Some(window) = store.windows.get_mut(&window_id) {
                        window.tabs.retain(|&id| id != tab_id);
                    }
                }

                // Update indices
                store.tab_to_table.remove(&tab_id);
                store.tab_to_window.remove(&tab_id);

                Ok(table_id)
            })
            .await?;

        if let Some(table_id) = table_id {
            self.registry
                .notify([TablesEvent::TableChanged { id: table_id }]);
        }
        self.registry.notify([TablesEvent::ListChanged]);

        Ok(())
    }
}
