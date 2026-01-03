use crate::interlude::*;

use crate::plugs::{manifest::PropKeyDisplayHint, PlugsRepo};
use crate::rt::triage::TriageConfig;
use tokio_util::sync::CancellationToken;

#[derive(Reconcile, Hydrate)]
pub struct ConfigStore {
    pub triage: TriageConfig,
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    pub prop_display: HashMap<String, ThroughJson<PropKeyDisplayHint>>,
}

impl Default for ConfigStore {
    fn default() -> Self {
        use crate::plugs::manifest::*;

        let mut key_configs = HashMap::new();

        key_configs.insert(
            "created_at".to_string(),
            PropKeyDisplayHint {
                always_visible: false,
                display_title: Some("Created At".to_string()),
                deets: PropKeyDisplayDeets::DateTime {
                    display_type: DateTimePropDisplayType::Relative,
                },
            }
            .into(),
        );
        key_configs.insert(
            "updated_at".to_string(),
            PropKeyDisplayHint {
                always_visible: false,
                display_title: Some("Updated At".to_string()),
                deets: PropKeyDisplayDeets::DateTime {
                    display_type: DateTimePropDisplayType::Relative,
                },
            }
            .into(),
        );

        Self {
            triage: TriageConfig::default(),
            prop_display: key_configs,
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
    plug_repo: Arc<PlugsRepo>,
    cancel_token: CancellationToken,
    _change_listener_tickets: Vec<utils_rs::am::changes::ChangeListenerRegistration>,
}

impl crate::repos::Repo for ConfigRepo {
    type Event = ConfigEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }
    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

impl ConfigRepo {
    pub async fn load(
        acx: AmCtx,
        app_doc_id: DocumentId,
        plug_repo: Arc<PlugsRepo>,
    ) -> Res<Arc<Self>> {
        let registry = crate::repos::ListenersRegistry::new();

        let store = ConfigStore::load(&acx, &app_doc_id).await?;
        let store = crate::stores::StoreHandle::new(store, acx.clone(), app_doc_id.clone());

        let broker = {
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
        let ticket = ConfigStore::register_change_listener(&acx, &broker, vec![], {
            move |notifs| {
                if let Err(err) = notif_tx.send(notifs) {
                    warn!("failed to send change notifications: {err}");
                }
            }
        })
        .await?;

        let cancel_token = CancellationToken::new();
        let repo = Self {
            acx: acx.clone(),
            app_doc_id: app_doc_id.clone(),
            store,
            registry: registry.clone(),
            plug_repo,
            cancel_token: cancel_token.clone(),
            _change_listener_tickets: vec![ticket],
        };
        let repo = Arc::new(repo);

        let _notif_worker = tokio::spawn({
            let repo = repo.clone();
            let cancel_token = cancel_token.clone();
            async move { repo.handle_notifs(notif_rx, cancel_token).await }
        });

        Ok(repo)
    }

    async fn handle_notifs(
        self: &Self,
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        loop {
            let _notifs = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    break;
                }
                msg = notif_rx.recv() => {
                    match msg {
                        Some(notifs) => notifs,
                        None => break,
                    }
                }
            };
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
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        self.store
            .mutate_sync(move |store| {
                store.triage.processors.insert(processor_id, processor);
            })
            .await?;
        Ok(())
    }

    pub async fn get_prop_display_hint(&self, key: String) -> Option<PropKeyDisplayHint> {
        let hint = self
            .store
            .query_sync(|store| store.prop_display.get(&key).cloned())
            .await;
        if let Some(hint) = hint {
            return Some(hint.0);
        }
        let hint = self.plug_repo.get_display_hint(&key).await;
        if let Some(hint) = hint {
            return Some(hint);
        }
        None
    }

    pub async fn list_display_hints(&self) -> HashMap<String, PropKeyDisplayHint> {
        let mut defaults: HashMap<_, _> = self
            .plug_repo
            .list_display_hints()
            .await
            .into_iter()
            .collect();

        self.store
            .query_sync(move |store| {
                for (key, val) in &store.prop_display {
                    defaults.insert(key.clone(), val.0.clone());
                }
                defaults
            })
            .await
    }

    pub async fn set_prop_display_hint(&self, key: String, hint: PropKeyDisplayHint) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        self.store
            .mutate_sync(move |store| {
                store.prop_display.insert(key, hint.into());
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
