use crate::interlude::*;
use crate::rt::triage::TriageConfig;
use std::collections::HashMap;

#[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DateTimeDisplayType {
    Relative,
    TimeOnly,
    DateOnly,
    TimeAndDate,
}

#[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum MetaTableKeyDisplayType {
    DateTime { display_type: DateTimeDisplayType },
    UnixPath,
    Title,
}

#[derive(Debug, Clone, Reconcile, Hydrate, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct MetaTableKeyConfig {
    pub always_visible: bool,
    pub display_type: MetaTableKeyDisplayType,
    pub display_title: Option<String>,
    pub show_title_editor: Option<bool>,
}

#[derive(Reconcile, Hydrate)]
pub struct ConfigStore {
    pub triage: TriageConfig,
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    pub meta_table_key_configs: HashMap<String, MetaTableKeyConfig>,
}

impl Default for ConfigStore {
    fn default() -> Self {
        let mut key_configs = HashMap::new();

        // Default configs for created_at and updated_at
        let datetime_config = MetaTableKeyDisplayType::DateTime {
            display_type: DateTimeDisplayType::Relative,
        };
        key_configs.insert(
            "created_at".to_string(),
            MetaTableKeyConfig {
                always_visible: false,
                display_type: datetime_config.clone(),
                display_title: Some("Created At".to_string()),
                show_title_editor: None,
            },
        );
        key_configs.insert(
            "updated_at".to_string(),
            MetaTableKeyConfig {
                always_visible: false,
                display_type: datetime_config.clone(),
                display_title: Some("Updated At".to_string()),
                show_title_editor: None,
            },
        );
        key_configs.insert(
            "path_generic".to_string(),
            MetaTableKeyConfig {
                always_visible: true,
                display_type: MetaTableKeyDisplayType::UnixPath,
                display_title: Some("Path".to_string()),
                show_title_editor: None,
            },
        );
        key_configs.insert(
            "title_generic".to_string(),
            MetaTableKeyConfig {
                always_visible: false,
                display_type: MetaTableKeyDisplayType::Title,
                display_title: Some("Title".to_string()),
                show_title_editor: Some(true),
            },
        );

        Self {
            triage: TriageConfig::default(),
            meta_table_key_configs: key_configs,
        }
    }
}

#[async_trait]
impl crate::stores::Store for ConfigStore {
    const PROP: &str = "config";
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
    _broker: Arc<utils_rs::am::changes::DocChangeBroker>,
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
        let store = crate::stores::StoreHandle::new(store, acx.clone(), app_doc_id.clone());

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
        ConfigStore::register_change_listener(&acx, &broker, vec![], {
            move |notifs| notif_tx.send(notifs).expect(ERROR_CHANNEL)
        })
        .await?;

        let repo = Self {
            acx: acx.clone(),
            app_doc_id: app_doc_id.clone(),
            store,
            registry: registry.clone(),
            _broker: broker,
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
        processor: crate::rt::triage::Processor,
    ) -> Res<()> {
        self.store
            .mutate_sync(move |store| {
                store.triage.processors.insert(processor_id, processor);
            })
            .await?;
        Ok(())
    }

    pub async fn get_meta_table_key_configs_sync(&self) -> HashMap<String, MetaTableKeyConfig> {
        self.store
            .query_sync(|store| store.meta_table_key_configs.clone())
            .await
    }

    pub async fn get_meta_table_key_config_sync(&self, key: String) -> Option<MetaTableKeyConfig> {
        self.store
            .query_sync(move |store| store.meta_table_key_configs.get(&key).cloned())
            .await
    }

    pub async fn set_meta_table_key_config(
        &self,
        key: String,
        config: MetaTableKeyConfig,
    ) -> Res<()> {
        self.store
            .mutate_sync(move |store| {
                store.meta_table_key_configs.insert(key, config);
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
