use samod::DocumentId;
use utils_rs::am::AmCtx;

mod store;

use crate::{
    ffi::{FfiError, SharedFfiCtx},
    interlude::*,
    tables::store::TablesStore,
};

#[derive(Debug, Clone, Reconcile, Hydrate, uniffi::Record, Patch, PartialEq)]
#[patch(attribute(derive(Debug, Default, uniffi::Record)))]
pub struct Window {
    #[key]
    pub id: Uuid,
    pub title: String,
    pub tabs: Vec<Uuid>,
    #[autosurgeon(missing = "Default::default")]
    pub selected_table: Option<Uuid>,
}

#[derive(Debug, Clone, Reconcile, Hydrate, uniffi::Record, PartialEq, Patch)]
#[patch(attribute(derive(Debug, Default, uniffi::Record)))]
pub struct Table {
    #[key]
    pub id: Uuid,
    pub title: String,
    pub tabs: Vec<Uuid>,
    #[autosurgeon(missing = "Default::default")]
    pub window: TableWindow,
    #[autosurgeon(missing = "Default::default")]
    pub selected_tab: Option<Uuid>,
}

#[derive(Debug, Clone, Reconcile, Hydrate, Default, PartialEq, uniffi::Enum)]
pub enum TableWindow {
    #[default]
    AllWindows,
    Specific {
        id: Uuid,
    },
}

#[derive(Debug, Clone, Reconcile, Hydrate, uniffi::Record, Patch, PartialEq)]
#[patch(attribute(derive(Debug, Default, uniffi::Record)))]
pub struct Tab {
    #[key]
    pub id: Uuid,
    pub title: String,
    pub panels: Vec<Uuid>,
    #[autosurgeon(missing = "Default::default")]
    pub selected_panel: Option<Uuid>,
}

#[derive(Debug, Clone, Reconcile, Hydrate, uniffi::Record, Patch, PartialEq)]
#[patch(attribute(derive(Debug, Default, uniffi::Record)))]
pub struct Panel {
    #[key]
    pub id: Uuid,
    pub title: String,
}

struct TablesRepo {
    acx: AmCtx,
    app_doc_id: DocumentId,
    store: crate::stores::StoreHandle<TablesStore>,
    // am: Arc<tokio::sync::RwLock<store::TablesStore>>,
    registry: Arc<crate::repos::ListenersRegistry>,
}

impl crate::repos::Repo for TablesRepo {
    type Event = TablesEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }
}

// Granular event enum for specific changes
#[derive(Debug, Clone, uniffi::Enum)]
pub enum TablesEvent {
    ListChanged,
    WindowChanged { id: Uuid },
    TabChanged { id: Uuid },
    PanelChanged { id: Uuid },
    TableChanged { id: Uuid },
}

#[derive(Debug, uniffi::Record)]
pub struct TablesPatches {
    pub tab_updates: Option<Vec<TabPatch>>,
    pub window_updates: Option<Vec<WindowPatch>>,
    pub panel_updates: Option<Vec<PanelPatch>>,
    pub table_updates: Option<Vec<TablePatch>>,
}

impl TablesRepo {
    async fn load(acx: AmCtx, app_doc_id: DocumentId) -> Res<Self> {
        let store = TablesStore::load(&acx, &app_doc_id).await?;
        let mut store = crate::stores::StoreHandle::new(store, (acx.clone(), app_doc_id.clone()));
        store
            .mutate(|store| {
                store.rebuild_indices();
                async {}
            })
            .await?;
        // Rebuild indices after loading
        // let am = Arc::new(tokio::sync::RwLock::new(am));
        let registry = crate::repos::ListenersRegistry::new();

        let repo = Self {
            acx,
            app_doc_id,
            store,
            registry: registry.clone(),
        };

        // Register change listener to automatically notify repo listeners
        TablesStore::register_change_listener(&repo.acx, repo.app_doc_id.clone(), {
            let registry = registry.clone();
            move |notifications| {
                // Analyze notifications to determine which specific events to send
                let mut events = Vec::new();

                for notification in notifications {
                    match &notification.patch.action {
                        automerge::PatchAction::PutMap { key, .. } => {
                            // Check if this is a specific item change
                            if let Ok(uuid) = Uuid::parse_str(&key) {
                                // Determine which type of item changed based on path
                                if notification.patch.path.len() >= 2 {
                                    match &notification.patch.path[1].1 {
                                        automerge::Prop::Map(path_key) => {
                                            match path_key.as_ref() {
                                                "windows" => events
                                                    .push(TablesEvent::WindowChanged { id: uuid }),
                                                "tables" => events
                                                    .push(TablesEvent::TableChanged { id: uuid }),
                                                "tabs" => events
                                                    .push(TablesEvent::TabChanged { id: uuid }),
                                                "panels" => events
                                                    .push(TablesEvent::PanelChanged { id: uuid }),
                                                _ => events.push(TablesEvent::ListChanged),
                                            }
                                        }
                                        _ => events.push(TablesEvent::ListChanged),
                                    }
                                } else {
                                    events.push(TablesEvent::ListChanged);
                                }
                            }
                        }
                        automerge::PatchAction::DeleteMap { key } => {
                            // Handle deletions
                            if let Ok(uuid) = Uuid::parse_str(&key) {
                                if notification.patch.path.len() >= 2 {
                                    match &notification.patch.path[1].1 {
                                        automerge::Prop::Map(path_key) => {
                                            match path_key.as_ref() {
                                                "windows" => events
                                                    .push(TablesEvent::WindowChanged { id: uuid }),
                                                "tables" => events
                                                    .push(TablesEvent::TableChanged { id: uuid }),
                                                "tabs" => events
                                                    .push(TablesEvent::TabChanged { id: uuid }),
                                                "panels" => events
                                                    .push(TablesEvent::PanelChanged { id: uuid }),
                                                _ => events.push(TablesEvent::ListChanged),
                                            }
                                        }
                                        _ => events.push(TablesEvent::ListChanged),
                                    }
                                } else {
                                    events.push(TablesEvent::ListChanged);
                                }
                            }
                        }
                        _ => {
                            // For other operations, send ListChanged
                            events.push(TablesEvent::ListChanged);
                        }
                    }
                }

                // Send events (deduplicate)
                let mut sent_events = std::collections::HashSet::new();
                for event in events {
                    if sent_events.insert(format!("{:?}", event)) {
                        registry.notify(event);
                    }
                }
            }
        })
        .await?;

        Ok(repo)
    }

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

    async fn get_window(&self, id: Uuid) -> Res<Option<Window>> {
        let am = self.am.read().await;
        Ok(am.windows.get(&id).cloned())
    }

    async fn set_window(&self, id: Uuid, val: Window) -> Res<Option<Window>> {
        let mut am = self.am.write().await;

        // Get old window to check for tab changes
        let old_window = am.windows.get(&id).cloned();
        let old_tabs = old_window
            .as_ref()
            .map(|w| w.tabs.clone())
            .unwrap_or_default();
        let new_tabs = val.tabs.clone();

        let ret = am.windows.insert(id, val);
        am.flush(&self.acx, &self.app_doc_id).await?;

        // Update tab-to-window index for changed tabs
        for tab_id in &old_tabs {
            if !new_tabs.contains(tab_id) {
                am.tab_to_window.remove(tab_id);
            }
        }
        for tab_id in &new_tabs {
            if !old_tabs.contains(tab_id) {
                am.tab_to_window.insert(*tab_id, id);
            }
        }

        self.registry.notify(TablesEvent::WindowChanged { id });
        self.registry.notify(TablesEvent::ListChanged);
        Ok(ret)
    }

    async fn list_windows(&self) -> Res<Vec<Window>> {
        let am = self.am.read().await;
        Ok(am.windows.values().cloned().collect())
    }

    async fn get_tab(&self, id: Uuid) -> Res<Option<Tab>> {
        let am = self.am.read().await;
        Ok(am.tabs.get(&id).cloned())
    }

    async fn set_tab(&self, id: Uuid, val: Tab) -> Res<Option<Tab>> {
        let mut am = self.am.write().await;

        // Get old tab to check for panel changes
        let old_tab = am.tabs.get(&id).cloned();
        let old_panels = old_tab
            .as_ref()
            .map(|t| t.panels.clone())
            .unwrap_or_default();
        let new_panels = val.panels.clone();

        let ret = am.tabs.insert(id, val);
        am.flush(&self.acx, &self.app_doc_id).await?;

        // Update panel-to-tab index for changed panels
        for panel_id in &old_panels {
            if !new_panels.contains(panel_id) {
                am.panel_to_tab.remove(panel_id);
            }
        }
        for panel_id in &new_panels {
            if !old_panels.contains(panel_id) {
                am.panel_to_tab.insert(*panel_id, id);
            }
        }

        // Send cascading events using indices
        self.registry.notify(TablesEvent::TabChanged { id });
        if let Some(table_id) = am.tab_to_table.get(&id).copied() {
            self.registry
                .notify(TablesEvent::TableChanged { id: table_id });
        }
        if let Some(window_id) = am.tab_to_window.get(&id).copied() {
            self.registry
                .notify(TablesEvent::WindowChanged { id: window_id });
        }
        self.registry.notify(TablesEvent::ListChanged);
        Ok(ret)
    }

    async fn list_tab(&self) -> Res<Vec<Tab>> {
        let am = self.am.read().await;
        Ok(am.tabs.values().cloned().collect())
    }

    async fn get_table(&self, id: Uuid) -> Res<Option<Table>> {
        let am = self.am.read().await;
        Ok(am.tables.get(&id).cloned())
    }

    async fn set_table(&self, id: Uuid, val: Table) -> Res<Option<Table>> {
        let mut am = self.am.write().await;

        // Get old table to check for tab changes
        let old_table = am.tables.get(&id).cloned();
        let old_tabs = old_table
            .as_ref()
            .map(|t| t.tabs.clone())
            .unwrap_or_default();
        let new_tabs = val.tabs.clone();

        let ret = am.tables.insert(id, val);
        am.flush(&self.acx, &self.app_doc_id).await?;

        // Update tab-to-table index for changed tabs
        for tab_id in &old_tabs {
            if !new_tabs.contains(tab_id) {
                am.tab_to_table.remove(tab_id);
            }
        }
        for tab_id in &new_tabs {
            if !old_tabs.contains(tab_id) {
                am.tab_to_table.insert(*tab_id, id);
            }
        }

        // Auto-create tab if all tabs were removed
        if new_tabs.is_empty() {
            self.auto_create_tab_for_table(id).await?;
        }

        self.registry.notify(TablesEvent::TableChanged { id });
        self.registry.notify(TablesEvent::ListChanged);
        Ok(ret)
    }

    async fn list_tables(&self) -> Res<Vec<Table>> {
        let am = self.am.read().await;
        let tables: Vec<Table> = am.tables.values().cloned().collect();

        // Auto-create table if none exist
        if tables.is_empty() {
            drop(am); // Release the read lock
            self.auto_create_default_table().await?;
            let am = self.am.read().await;
            Ok(am.tables.values().cloned().collect())
        } else {
            Ok(tables)
        }
    }

    async fn get_panel(&self, id: Uuid) -> Res<Option<Panel>> {
        let am = self.am.read().await;
        Ok(am.panels.get(&id).cloned())
    }

    async fn set_panel(&self, id: Uuid, val: Panel) -> Res<Option<Panel>> {
        let mut am = self.am.write().await;
        let ret = am.panels.insert(id, val);
        am.flush(&self.acx, &self.app_doc_id).await?;

        // Send cascading events using indices
        self.registry.notify(TablesEvent::PanelChanged { id });
        if let Some(tab_id) = am.panel_to_tab.get(&id).copied() {
            self.registry.notify(TablesEvent::TabChanged { id: tab_id });
            if let Some(table_id) = am.tab_to_table.get(&tab_id).copied() {
                self.registry
                    .notify(TablesEvent::TableChanged { id: table_id });
            }
            if let Some(window_id) = am.tab_to_window.get(&tab_id).copied() {
                self.registry
                    .notify(TablesEvent::WindowChanged { id: window_id });
            }
        }
        self.registry.notify(TablesEvent::ListChanged);
        Ok(ret)
    }

    async fn list_panel(&self) -> Res<Vec<Panel>> {
        let am = self.am.read().await;
        Ok(am.panels.values().cloned().collect())
    }

    async fn update_batch(&self, patches: TablesPatches) -> Res<()> {
        let mut am = self.am.write().await;

        // Apply tab updates
        if let Some(tab_updates) = patches.tab_updates {
            for tab_patch in tab_updates {
                if let Some(id) = tab_patch.id {
                    if let Some(tab) = am.tabs.get_mut(&id) {
                        tab.apply(tab_patch);
                    }
                }
            }
        }

        // Apply window updates
        if let Some(window_updates) = patches.window_updates {
            for window_patch in window_updates {
                if let Some(id) = window_patch.id {
                    if let Some(window) = am.windows.get_mut(&id) {
                        window.apply(window_patch);
                    }
                }
            }
        }

        // Apply panel updates
        if let Some(panel_updates) = patches.panel_updates {
            for panel_patch in panel_updates {
                if let Some(id) = panel_patch.id {
                    if let Some(panel) = am.panels.get_mut(&id) {
                        panel.apply(panel_patch);
                    }
                }
            }
        }

        // Apply table updates
        if let Some(table_updates) = patches.table_updates {
            for table_patch in table_updates {
                if let Some(id) = table_patch.id {
                    if let Some(table) = am.tables.get_mut(&id) {
                        table.apply(table_patch);
                    }
                }
            }
        }

        // Rebuild indices after all updates
        am.rebuild_indices();

        am.flush(&self.acx, &self.app_doc_id).await?;
        self.registry.notify(TablesEvent::ListChanged);
        Ok(())
    }

    // Auto-create a default table with window, tab, and panel
    async fn auto_create_default_table(&self) -> Res<()> {
        let mut am = self.am.write().await;

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
        };
        am.windows.insert(window_id, window);

        // Create table
        let table = Table {
            id: table_id,
            title: "Main Table".to_string(),
            tabs: vec![tab_id],
            window: TableWindow::Specific { id: window_id },
            selected_tab: Some(tab_id),
        };
        am.tables.insert(table_id, table);

        // Create tab
        let tab = Tab {
            id: tab_id,
            title: "Main Tab".to_string(),
            panels: vec![panel_id],
            selected_panel: Some(panel_id),
        };
        am.tabs.insert(tab_id, tab);

        // Create panel
        let panel = Panel {
            id: panel_id,
            title: "Main Panel".to_string(),
        };
        am.panels.insert(panel_id, panel);

        // Update indices
        am.tab_to_table.insert(tab_id, table_id);
        am.tab_to_window.insert(tab_id, window_id);
        am.panel_to_tab.insert(panel_id, tab_id);

        am.flush(&self.acx, &self.app_doc_id).await?;
        self.registry.notify(TablesEvent::ListChanged);
        Ok(())
    }

    // Auto-create a tab for a table when all tabs are removed
    async fn auto_create_tab_for_table(&self, table_id: Uuid) -> Res<()> {
        let mut am = self.am.write().await;

        // Get the table to find its window
        let table = am.tables.get(&table_id).ok_or_eyre("Table not found")?;
        let window_id = match &table.window {
            TableWindow::Specific { id } => *id,
            TableWindow::AllWindows => {
                // Find the first window or create one
                if let Some((&id, _)) = am.windows.iter().next() {
                    id
                } else {
                    // Create a new window
                    let new_window_id = Uuid::new_v4();
                    let window = Window {
                        id: new_window_id,
                        title: "Main Window".to_string(),
                        tabs: vec![],
                        selected_table: Some(table_id),
                    };
                    am.windows.insert(new_window_id, window);
                    new_window_id
                }
            }
        };

        let tab_id = Uuid::new_v4();
        let panel_id = Uuid::new_v4();

        // Create tab
        let tab = Tab {
            id: tab_id,
            title: "New Tab".to_string(),
            panels: vec![panel_id],
            selected_panel: Some(panel_id),
        };
        am.tabs.insert(tab_id, tab);

        // Create panel
        let panel = Panel {
            id: panel_id,
            title: "New Panel".to_string(),
        };
        am.panels.insert(panel_id, panel);

        // Update table to include the new tab
        if let Some(table) = am.tables.get_mut(&table_id) {
            table.tabs.push(tab_id);
            table.selected_tab = Some(tab_id);
        }

        // Update window to include the new tab
        if let Some(window) = am.windows.get_mut(&window_id) {
            if !window.tabs.contains(&tab_id) {
                window.tabs.push(tab_id);
            }
        }

        // Update indices
        am.tab_to_table.insert(tab_id, table_id);
        am.tab_to_window.insert(tab_id, window_id);
        am.panel_to_tab.insert(panel_id, tab_id);

        am.flush(&self.acx, &self.app_doc_id).await?;
        self.registry
            .notify(TablesEvent::TableChanged { id: table_id });
        self.registry.notify(TablesEvent::ListChanged);
        Ok(())
    }

    // Get the selected table from the first window
    async fn get_selected_table(&self) -> Res<Option<Table>> {
        let am = self.am.read().await;

        // Find the first window with a selected table
        for window in am.windows.values() {
            if let Some(selected_table_id) = window.selected_table {
                if let Some(table) = am.tables.get(&selected_table_id) {
                    return Ok(Some(table.clone()));
                }
            }
        }

        // If no selected table found, return the first table
        if let Some((_, table)) = am.tables.iter().next() {
            Ok(Some(table.clone()))
        } else {
            Ok(None)
        }
    }

    // Create a new table with a default tab and panel
    async fn create_new_table(&self) -> Res<Table> {
        let mut am = self.am.write().await;

        let table_id = Uuid::new_v4();
        let tab_id = Uuid::new_v4();
        let panel_id = Uuid::new_v4();

        // Find or create a window for this table
        let window_id = if let Some((&id, _)) = am.windows.iter().next() {
            id
        } else {
            // Create a new window
            let new_window_id = Uuid::new_v4();
            let window = Window {
                id: new_window_id,
                title: "Main Window".to_string(),
                tabs: vec![tab_id],
                selected_table: Some(table_id),
            };
            am.windows.insert(new_window_id, window);
            new_window_id
        };

        // Create table
        let table = Table {
            id: table_id,
            title: format!("Table {}", am.tables.len() + 1),
            tabs: vec![tab_id],
            window: TableWindow::Specific { id: window_id },
            selected_tab: Some(tab_id),
        };
        am.tables.insert(table_id, table.clone());

        // Create tab
        let tab = Tab {
            id: tab_id,
            title: format!("Tab {}", am.tabs.len() + 1),
            panels: vec![panel_id],
            selected_panel: Some(panel_id),
        };
        am.tabs.insert(tab_id, tab.clone());

        // Create panel
        let panel = Panel {
            id: panel_id,
            title: format!("Panel {}", am.panels.len() + 1),
        };
        am.panels.insert(panel_id, panel);

        // Update window to include the new tab
        if let Some(window) = am.windows.get_mut(&window_id) {
            if !window.tabs.contains(&tab_id) {
                window.tabs.push(tab_id);
            }
            window.selected_table = Some(table_id);
        }

        // Update indices
        am.tab_to_table.insert(tab_id, table_id);
        am.tab_to_window.insert(tab_id, window_id);
        am.panel_to_tab.insert(panel_id, tab_id);

        am.flush(&self.acx, &self.app_doc_id).await?;
        self.registry
            .notify(TablesEvent::TableChanged { id: table_id });
        self.registry.notify(TablesEvent::ListChanged);

        Ok(table)
    }

    // Create a new tab for an existing table
    async fn create_new_tab(&self, table_id: Uuid) -> Res<Tab> {
        let mut am = self.am.write().await;

        // Get the table to find its window
        let table = am.tables.get(&table_id).ok_or_eyre("Table not found")?;
        let window_id = match &table.window {
            TableWindow::Specific { id } => *id,
            TableWindow::AllWindows => {
                // Find the first window or create one
                if let Some((&id, _)) = am.windows.iter().next() {
                    id
                } else {
                    // Create a new window
                    let new_window_id = Uuid::new_v4();
                    let window = Window {
                        id: new_window_id,
                        title: "Main Window".to_string(),
                        tabs: vec![],
                        selected_table: Some(table_id),
                    };
                    am.windows.insert(new_window_id, window);
                    new_window_id
                }
            }
        };

        let tab_id = Uuid::new_v4();
        let panel_id = Uuid::new_v4();

        // Create tab
        let tab = Tab {
            id: tab_id,
            title: format!("Tab {}", am.tabs.len() + 1),
            panels: vec![panel_id],
            selected_panel: Some(panel_id),
        };
        am.tabs.insert(tab_id, tab.clone());

        // Create panel
        let panel = Panel {
            id: panel_id,
            title: format!("Panel {}", am.panels.len() + 1),
        };
        am.panels.insert(panel_id, panel);

        // Update table to include the new tab
        if let Some(table) = am.tables.get_mut(&table_id) {
            table.tabs.push(tab_id);
            table.selected_tab = Some(tab_id);
        }

        // Update window to include the new tab
        if let Some(window) = am.windows.get_mut(&window_id) {
            if !window.tabs.contains(&tab_id) {
                window.tabs.push(tab_id);
            }
        }

        // Update indices
        am.tab_to_table.insert(tab_id, table_id);
        am.tab_to_window.insert(tab_id, window_id);
        am.panel_to_tab.insert(panel_id, tab_id);

        am.flush(&self.fcx.cx).await?;
        self.registry
            .notify(TablesEvent::TableChanged { id: table_id });
        self.registry.notify(TablesEvent::ListChanged);

        Ok(tab)
    }

    // Remove a tab and its panel
    async fn remove_tab(&self, tab_id: Uuid) -> Res<()> {
        let mut am = self.am.write().await;

        // Get the tab to find its panels
        let tab = am.tabs.get(&tab_id).ok_or_eyre("Tab not found")?;
        let panel_ids = tab.panels.clone();

        // Find which table and window contain this tab
        let table_id = am.tab_to_table.get(&tab_id).copied();
        let window_id = am.tab_to_window.get(&tab_id).copied();

        // Remove the tab
        am.tabs.remove(&tab_id);

        // Remove all panels in this tab
        for panel_id in panel_ids {
            am.panels.remove(&panel_id);
            am.panel_to_tab.remove(&panel_id);
        }

        // Update table to remove the tab
        if let Some(table_id) = table_id {
            if let Some(table) = am.tables.get_mut(&table_id) {
                table.tabs.retain(|&id| id != tab_id);
                // If this was the selected tab, select another one or clear selection
                if table.selected_tab == Some(tab_id) {
                    table.selected_tab = table.tabs.first().copied();
                }

                // Auto-create a new tab if this was the last tab
                if table.tabs.is_empty() {
                    drop(am); // Release the write lock
                    self.auto_create_tab_for_table(table_id).await?;
                    return Ok(());
                }
            }
        }

        // Update window to remove the tab
        if let Some(window_id) = window_id {
            if let Some(window) = am.windows.get_mut(&window_id) {
                window.tabs.retain(|&id| id != tab_id);
            }
        }

        // Update indices
        am.tab_to_table.remove(&tab_id);
        am.tab_to_window.remove(&tab_id);

        am.flush(&self.fcx.cx).await?;
        if let Some(table_id) = table_id {
            self.registry
                .notify(TablesEvent::TableChanged { id: table_id });
        }
        self.registry.notify(TablesEvent::ListChanged);

        Ok(())
    }
}

#[derive(uniffi::Object)]
struct TablesRepoFfi {
    fcx: SharedFfiCtx,
    repo: TablesRepo,
}

impl crate::repos::Repo for TablesRepoFfi {
    type Event = TablesEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.repo.registry
    }
}

crate::uniffi_repo_listeners!(TablesRepoFfi, TablesEvent);

#[uniffi::export]
impl TablesRepoFfi {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx))]
    async fn for_ffi(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let cx = fcx.clone();
        let this = fcx.do_on_rt(Self::load(cx)).await?;
        Ok(this)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_get_window(self: Arc<Self>, id: Uuid) -> Result<Option<Window>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.get_window(id).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self, window))]
    async fn ffi_set_window(
        self: Arc<Self>,
        id: Uuid,
        window: Window,
    ) -> Result<Option<Window>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.set_window(id, window).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_list_windows(self: Arc<Self>) -> Result<Vec<Window>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.list_windows().await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_get_tab(self: Arc<Self>, id: Uuid) -> Result<Option<Tab>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.get_tab(id).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self, tab))]
    async fn ffi_set_tab(self: Arc<Self>, id: Uuid, tab: Tab) -> Result<Option<Tab>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.set_tab(id, tab).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_list_tabs(self: Arc<Self>) -> Result<Vec<Tab>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.list_tab().await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_get_panel(self: Arc<Self>, id: Uuid) -> Result<Option<Panel>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.get_panel(id).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self, panel))]
    async fn ffi_set_panel(
        self: Arc<Self>,
        id: Uuid,
        panel: Panel,
    ) -> Result<Option<Panel>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.set_panel(id, panel).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_list_panels(self: Arc<Self>) -> Result<Vec<Panel>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.list_panel().await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_get_table(self: Arc<Self>, id: Uuid) -> Result<Option<Table>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.get_table(id).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self, table))]
    async fn ffi_set_table(
        self: Arc<Self>,
        id: Uuid,
        table: Table,
    ) -> Result<Option<Table>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.set_table(id, table).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_list_tables(self: Arc<Self>) -> Result<Vec<Table>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.list_tables().await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_get_selected_table(self: Arc<Self>) -> Result<Option<Table>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.get_selected_table().await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self, patches))]
    async fn ffi_update_batch(self: Arc<Self>, patches: TablesPatches) -> Result<(), FfiError> {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.update_batch(patches).await })
            .await?;
        Ok(())
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_create_new_table(self: Arc<Self>) -> Result<Table, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.create_new_table().await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_create_new_tab(self: Arc<Self>, table_id: Uuid) -> Result<Tab, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.create_new_tab(table_id).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_remove_tab(self: Arc<Self>, tab_id: Uuid) -> Result<(), FfiError> {
        let this = self.clone();
        self.fcx
            .do_on_rt(async move { this.remove_tab(tab_id).await })
            .await?;
        Ok(())
    }
}
