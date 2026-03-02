use crate::interlude::*;

use tokio_util::sync::CancellationToken;

use crate::plugs::{manifest::FacetDisplayHint, PlugsRepo};
use crate::stores::Versioned;

#[derive(Reconcile, Hydrate, Clone)]
pub struct ConfigStore {
    pub facet_display: HashMap<String, Versioned<ThroughJson<FacetDisplayHint>>>,
    pub users: HashMap<String, Versioned<ThroughJson<UserMeta>>>,
    pub mltools: Versioned<ThroughJson<mltools::Config>>,
    pub global_props_doc_id: Versioned<Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct UserMeta {
    #[autosurgeon(with = "am_utils_rs::codecs::path")]
    pub user_path: daybook_types::doc::UserPath,
    #[autosurgeon(with = "am_utils_rs::codecs::date")]
    pub seen_at: Timestamp,
}

impl Default for ConfigStore {
    fn default() -> Self {
        use crate::plugs::manifest::*;

        let mut key_configs = HashMap::new();

        key_configs.insert(
            "created_at".to_string(),
            Versioned {
                vtag: VersionTag::nil(),
                val: FacetDisplayHint {
                    always_visible: false,
                    display_title: Some("Created At".to_string()),
                    deets: FacetKeyDisplayDeets::DateTime {
                        display_type: DateTimeFacetDisplayType::Relative,
                    },
                }
                .into(),
            },
        );
        key_configs.insert(
            "updated_at".to_string(),
            Versioned {
                vtag: VersionTag::nil(),
                val: FacetDisplayHint {
                    always_visible: false,
                    display_title: Some("Updated At".to_string()),
                    deets: FacetKeyDisplayDeets::DateTime {
                        display_type: DateTimeFacetDisplayType::Relative,
                    },
                }
                .into(),
            },
        );

        Self {
            facet_display: key_configs,
            users: HashMap::new(),
            mltools: Versioned {
                vtag: VersionTag::nil(),
                val: mltools::Config {
                    ocr: mltools::OcrConfig { backends: vec![] },
                    embed: mltools::EmbedConfig { backends: vec![] },
                    image_embed: mltools::ImageEmbedConfig { backends: vec![] },
                    llm: mltools::LlmConfig { backends: vec![] },
                }
                .into(),
            },
            global_props_doc_id: Versioned {
                vtag: VersionTag::nil(),
                val: None,
            },
        }
    }
}

#[async_trait]
impl crate::stores::AmStore for ConfigStore {
    fn prop() -> Cow<'static, str> {
        "config".into()
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum ConfigEvent {
    Changed { heads: ChangeHashSet },
    SyncDevicesChanged,
}

pub struct ConfigRepo {
    acx: AmCtx,
    app_doc_id: DocumentId,
    app_am_handle: samod::DocHandle,
    store: crate::stores::AmStoreHandle<ConfigStore>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    plug_repo: Arc<PlugsRepo>,
    local_user_path: daybook_types::doc::UserPath,
    local_actor_id: ActorId,
    sql_pool: sqlx::SqlitePool,
    cancel_token: CancellationToken,
    global_props_doc_init_lock: tokio::sync::Mutex<()>,
    _change_listener_tickets: Vec<am_utils_rs::changes::ChangeListenerRegistration>,
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
        local_user_path: daybook_types::doc::UserPath,
        sql_pool: sqlx::SqlitePool,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let registry = crate::repos::ListenersRegistry::new();
        let store_val = ConfigStore::load(&acx, &app_doc_id).await?;
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);

        let store = crate::stores::AmStoreHandle::new(
            store_val,
            acx.clone(),
            app_doc_id.clone(),
            local_actor_id.clone(),
        );

        store
            .mutate_sync(|store| {
                store
                    .users
                    .entry(local_user_path.to_string_lossy().into_owned())
                    .or_insert_with(|| {
                        Versioned::mint(
                            local_actor_id.clone(),
                            UserMeta {
                                user_path: local_user_path.clone(),
                                seen_at: Timestamp::now(),
                            }
                            .into(),
                        )
                    });
            })
            .await?;

        let app_am_handle = acx
            .find_doc(&app_doc_id)
            .await?
            .ok_or_eyre("unable to find app doc in am")?;

        let (broker, broker_stop) = acx.change_manager().add_doc(app_am_handle.clone()).await?;

        let (notif_tx, notif_rx) =
            tokio::sync::mpsc::unbounded_channel::<Vec<am_utils_rs::changes::ChangeNotification>>();
        // Register change listener to automatically notify repo listeners
        let ticket = ConfigStore::register_change_listener(&acx, &broker, vec![], {
            move |notifs| {
                if let Err(err) = notif_tx.send(notifs) {
                    warn!("failed to send change notifications: {err}");
                }
            }
        })
        .await?;

        let main_cancel_token = CancellationToken::new();
        let repo = Self {
            acx: acx.clone(),
            app_doc_id: app_doc_id.clone(),
            app_am_handle,
            store,
            registry: Arc::clone(&registry),
            plug_repo,
            local_user_path,
            local_actor_id,
            sql_pool,
            cancel_token: main_cancel_token.child_token(),
            global_props_doc_init_lock: tokio::sync::Mutex::new(()),
            _change_listener_tickets: vec![ticket],
        };
        let repo = Arc::new(repo);

        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let cancel_token = main_cancel_token.clone();
            async move {
                repo.notifs_loop(notif_rx, cancel_token)
                    .await
                    .expect("error handling notifs")
            }
        });

        Ok((
            repo,
            crate::repos::RepoStopToken {
                cancel_token: main_cancel_token,
                worker_handle: Some(worker_handle),
                broker_stop_tokens: vec![broker_stop],
            },
        ))
    }

    async fn notifs_loop(
        &self,
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<
            Vec<am_utils_rs::changes::ChangeNotification>,
        >,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        // FIXME: this is suspicous
        let mut events = vec![];
        loop {
            let notifs = tokio::select! {
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

            events.clear();
            let mut last_heads = None;
            for notif in notifs {
                let len = events.len();
                self.events_for_patch(
                    &notif.patch,
                    &notif.heads,
                    &mut events,
                    Some(self.local_actor_id.clone()),
                )
                .await?;
                // events were added
                if len != events.len() {
                    last_heads = Some(ChangeHashSet(Arc::clone(&notif.heads)));
                }
            }
            // for event in &events {
            //     match event {
            //         ConfigEvent::Changed { heads } => todo!(),
            //         ConfigEvent::SyncDevicesChanged => todo!(),
            //     }
            // }

            if let Some(heads) = last_heads {
                let (new_store, _) = self
                    .acx
                    .hydrate_path_at_heads::<ConfigStore>(
                        &self.app_doc_id,
                        &heads,
                        automerge::ROOT,
                        vec![ConfigStore::prop().into()],
                    )
                    .await?
                    .expect(ERROR_INVALID_PATCH);

                self.store
                    .mutate_sync(|store| {
                        store.facet_display = new_store.facet_display;
                        store.users = new_store.users;
                        store.mltools = new_store.mltools;
                        store.global_props_doc_id = new_store.global_props_doc_id;
                    })
                    .await?;

                self.registry.notify(events.drain(..));
            }
        }
        Ok(())
    }

    pub async fn diff_events(
        &self,
        from: ChangeHashSet,
        to: Option<ChangeHashSet>,
    ) -> Res<Vec<ConfigEvent>> {
        let (patches, heads) = self.app_am_handle.with_document(|am_doc| {
            let heads = if let Some(ref to_set) = to {
                to_set.clone()
            } else {
                ChangeHashSet(am_doc.get_heads().into())
            };
            let patches = am_doc
                .diff_obj(&automerge::ROOT, &from, &heads, true)
                .expect("diff_obj failed");
            (patches, heads)
        });
        let heads = heads.0;
        let mut events = vec![];
        for patch in patches {
            self.events_for_patch(&patch, &heads, &mut events, None)
                .await?;
        }
        Ok(events)
    }

    async fn events_for_patch(
        &self,
        patch: &automerge::Patch,
        patch_heads: &Arc<[automerge::ChangeHash]>,
        out: &mut Vec<ConfigEvent>,
        exclude_actor_id: Option<ActorId>,
    ) -> Res<()> {
        let heads = ChangeHashSet(Arc::clone(patch_heads));

        match &patch.action {
            automerge::PatchAction::PutMap {
                key,
                value: (val, _),
                ..
            } if patch.path.len() == 2 && key == "vtag" => {
                let Some((_obj, automerge::Prop::Map(section_key))) = patch.path.get(1) else {
                    return Ok(());
                };
                let vtag = match val {
                    automerge::Value::Scalar(scalar) => match &**scalar {
                        automerge::ScalarValue::Bytes(bytes) => bytes,
                        _ => return Ok(()),
                    },
                    _ => return Ok(()),
                };
                let vtag = VersionTag::hydrate_bytes(vtag)?;
                if Some(vtag.actor_id) == exclude_actor_id {
                    return Ok(());
                }

                if matches!(
                    section_key.as_ref(),
                    "facet_display" | "users" | "mltools" | "global_props_doc_id"
                ) {
                    out.push(ConfigEvent::Changed { heads });
                }
            }
            _ => {}
        }
        Ok(())
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

    pub async fn get_facet_display_hint(&self, key: String) -> Option<FacetDisplayHint> {
        let hint = self
            .store
            .query_sync(|store| store.facet_display.get(&key).cloned())
            .await;
        if let Some(hint) = hint {
            return Some(hint.val.0);
        }
        let hint = self.plug_repo.get_display_hint(&key).await;
        if let Some(hint) = hint {
            return Some(hint);
        }
        None
    }

    pub async fn list_display_hints(&self) -> HashMap<String, FacetDisplayHint> {
        let mut defaults: HashMap<_, _> = self
            .plug_repo
            .list_display_hints()
            .await
            .into_iter()
            .collect();

        self.store
            .query_sync(move |store| {
                for (key, val) in &store.facet_display {
                    defaults.insert(key.clone(), val.0.clone());
                }
                defaults
            })
            .await
    }

    pub async fn set_facet_display_hint(&self, key: String, hint: FacetDisplayHint) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        self.store
            .mutate_sync(move |store| {
                let Some(old) = store.facet_display.get_mut(&key) else {
                    store.facet_display.insert(
                        key,
                        Versioned::mint(self.local_actor_id.clone(), hint.into()),
                    );
                    return;
                };
                old.replace(self.local_actor_id.clone(), hint.into());
            })
            .await?;
        Ok(())
    }

    pub async fn get_mltools_config(&self) -> mltools::Config {
        self.store
            .query_sync(|store| store.mltools.val.0.clone())
            .await
    }

    pub async fn set_mltools_config(&self, config: mltools::Config) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }

        self.store
            .mutate_sync(move |store| {
                store
                    .mltools
                    .replace(self.local_actor_id.clone(), config.into());
            })
            .await?;
        Ok(())
    }

    pub async fn get_global_props_doc_id(&self) -> Option<daybook_types::doc::DocId> {
        self.store
            .query_sync(|store| store.global_props_doc_id.val.clone())
            .await
    }

    pub async fn set_global_props_doc_id(&self, doc_id: daybook_types::doc::DocId) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        self.store
            .mutate_sync(move |store| {
                store
                    .global_props_doc_id
                    .replace(self.local_actor_id.clone(), Some(doc_id));
            })
            .await?;
        Ok(())
    }

    pub async fn get_or_init_global_props_doc_id(
        &self,
        drawer_repo: &crate::drawer::DrawerRepo,
    ) -> Res<daybook_types::doc::DocId> {
        if let Some(doc_id) = self.get_global_props_doc_id().await {
            return Ok(doc_id);
        }
        let _guard = self.global_props_doc_init_lock.lock().await;
        if let Some(doc_id) = self.get_global_props_doc_id().await {
            return Ok(doc_id);
        }
        let doc_id = drawer_repo
            .add(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPath::from("main"),
                facets: HashMap::new(),
                user_path: Some(self.local_user_path.clone()),
            })
            .await?;
        self.set_global_props_doc_id(doc_id.clone()).await?;
        Ok(doc_id)
    }

    pub async fn get_actor_user_path(
        &self,
        actor_id: &automerge::ActorId,
    ) -> Option<daybook_types::doc::UserPath> {
        let actor_id_str = actor_id.to_string();
        self.store
            .query_sync(move |store| {
                store
                    .users
                    .get(&actor_id_str)
                    .map(|doc| doc.user_path.clone())
            })
            .await
    }

    pub async fn list_known_sync_devices(&self) -> Res<Vec<crate::app::globals::SyncDeviceEntry>> {
        let config = crate::app::globals::get_sync_config(&self.sql_pool).await?;
        Ok(config.known_devices)
    }

    pub async fn upsert_known_sync_device(
        &self,
        device: crate::app::globals::SyncDeviceEntry,
    ) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let mut config = crate::app::globals::get_sync_config(&self.sql_pool).await?;
        if let Some(existing) = config
            .known_devices
            .iter_mut()
            .find(|entry| entry.endpoint_id == device.endpoint_id)
        {
            *existing = device;
        } else {
            config.known_devices.push(device);
        }
        crate::app::globals::set_sync_config(&self.sql_pool, &config).await?;
        self.registry.notify([ConfigEvent::SyncDevicesChanged]);
        Ok(())
    }

    pub async fn remove_known_sync_device(&self, endpoint_id: &iroh::EndpointId) -> Res<bool> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let mut config = crate::app::globals::get_sync_config(&self.sql_pool).await?;
        let before = config.known_devices.len();
        config
            .known_devices
            .retain(|entry| &entry.endpoint_id != endpoint_id);
        let removed = config.known_devices.len() != before;
        if removed {
            crate::app::globals::set_sync_config(&self.sql_pool, &config).await?;
            self.registry.notify([ConfigEvent::SyncDevicesChanged]);
        }
        Ok(removed)
    }

    pub async fn ensure_local_sync_device(
        &self,
        endpoint_id: iroh::EndpointId,
        device_name: &str,
    ) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let mut config = crate::app::globals::get_sync_config(&self.sql_pool).await?;
        if config
            .known_devices
            .iter()
            .any(|entry| entry.endpoint_id == endpoint_id)
        {
            return Ok(());
        }
        config
            .known_devices
            .push(crate::app::globals::SyncDeviceEntry {
                endpoint_id,
                name: device_name.to_string(),
                added_at: jiff::Timestamp::now(),
                last_connected_at: None,
            });
        crate::app::globals::set_sync_config(&self.sql_pool, &config).await?;
        self.registry.notify([ConfigEvent::SyncDevicesChanged]);
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
            super::ConfigStore::prop().as_ref(),
            super::ConfigStore::default(),
        )?;
        Ok(doc.save_nocompress())
    }
}
