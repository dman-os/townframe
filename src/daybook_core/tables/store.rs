use super::*;

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

impl crate::stores::Store for TablesStore {
    type FlushArgs = (AmCtx, DocumentId);

    async fn flush(&self, (acx, app_doc_id): &Self::FlushArgs) -> Res<()> {
        acx.reconcile_prop(app_doc_id, automerge::ROOT, Self::PROP, self)
            .await
    }
}

impl TablesStore {
    pub const PROP: &str = "tables";

    pub async fn load(acx: &AmCtx, app_doc_id: &DocumentId) -> Res<Self> {
        acx.hydrate_path::<Self>(app_doc_id, automerge::ROOT, vec![Self::PROP.into()])
            .await?
            .ok_or_eyre("unable to find obj in am")
    }

    /// Register a change listener for tables changes
    pub async fn register_change_listener<F>(
        acx: &AmCtx,
        app_doc_id: DocumentId,
        on_change: F,
    ) -> Res<()>
    where
        F: Fn(Vec<utils_rs::am::changes::ChangeNotification>) + Send + Sync + 'static,
    {
        acx.change_manager()
            .add_listener(
                utils_rs::am::changes::ChangeFilter {
                    path: vec![Self::PROP.into()],
                    doc_id: Some(app_doc_id),
                },
                on_change,
            )
            .await;
        Ok(())
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
