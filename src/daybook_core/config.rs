use crate::interlude::*;

use daybook_types::manifest::FacetDisplayHint;
use tokio_util::sync::CancellationToken;

use crate::plugs::PlugsRepo;
use crate::stores::Versioned;

#[derive(Reconcile, Hydrate, Clone)]
pub struct ConfigStore {
    pub facet_display: HashMap<String, Versioned<ThroughJson<FacetDisplayHint>>>,
    pub facet_display_deleted: HashMap<String, Vec<VersionTag>>,
    pub users: HashMap<String, Versioned<ThroughJson<UserMeta>>>,
    pub users_deleted: HashMap<String, Vec<VersionTag>>,
    pub mltools: Versioned<ThroughJson<mltools::Config>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct UserMeta {
    #[autosurgeon(with = "am_utils_rs::codecs::utf8_path")]
    pub user_path: daybook_types::doc::UserPath,
    #[autosurgeon(with = "am_utils_rs::codecs::date")]
    pub seen_at: Timestamp,
}

impl Default for ConfigStore {
    fn default() -> Self {
        use daybook_types::manifest::*;

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
            facet_display_deleted: HashMap::new(),
            users: HashMap::new(),
            users_deleted: HashMap::new(),
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
    Changed {
        heads: ChangeHashSet,
        origin: crate::event_origin::SwitchEventOrigin,
    },
    SyncDevicesChanged {
        origin: crate::event_origin::SwitchEventOrigin,
    },
}

pub struct ConfigRepo {
    big_repo: SharedBigRepo,
    app_doc_id: DocumentId,
    app_am_handle: am_utils_rs::repo::BigDocHandle,
    store: crate::stores::AmStoreHandle<ConfigStore>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    plug_repo: Arc<PlugsRepo>,
    local_actor_id: ActorId,
    local_peer_id: am_utils_rs::repo::PeerId,
    sql_pool: sqlx::SqlitePool,
    cancel_token: CancellationToken,
    sync_config_lock: tokio::sync::Mutex<()>,
    _change_listener_tickets: Vec<am_utils_rs::repo::BigRepoChangeListenerRegistration>,
    _change_broker_leases: Vec<Arc<am_utils_rs::repo::BigRepoDocChangeBrokerLease>>,
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
    fn local_origin(&self) -> crate::event_origin::SwitchEventOrigin {
        crate::event_origin::SwitchEventOrigin::Local {
            actor_id: self.local_actor_id.to_string(),
        }
    }

    fn origin_from_live(
        &self,
        live_origin: Option<&am_utils_rs::repo::BigRepoChangeOrigin>,
    ) -> crate::event_origin::SwitchEventOrigin {
        match live_origin {
            Some(am_utils_rs::repo::BigRepoChangeOrigin::Local) => self.local_origin(),
            Some(am_utils_rs::repo::BigRepoChangeOrigin::Remote { peer_id, .. }) => {
                crate::event_origin::SwitchEventOrigin::Remote {
                    peer_id: peer_id.to_string(),
                }
            }
            Some(am_utils_rs::repo::BigRepoChangeOrigin::Bootstrap) => {
                crate::event_origin::SwitchEventOrigin::Bootstrap
            }
            None => crate::event_origin::SwitchEventOrigin::Remote {
                peer_id: "unknown".to_string(),
            },
        }
    }

    async fn latest_deleted_actor(
        &self,
        deleted_prop: &str,
        key: &str,
        heads: &Arc<[automerge::ChangeHash]>,
    ) -> Res<Option<ActorId>> {
        let Some((tags, _)) = self
            .big_repo
            .hydrate_path_at_heads::<Vec<VersionTag>>(
                &self.app_doc_id,
                heads,
                automerge::ROOT,
                vec![
                    ConfigStore::prop().into(),
                    std::borrow::Cow::<str>::Owned(deleted_prop.to_string()).into(),
                    autosurgeon::Prop::Key(key.to_string().into()),
                ],
            )
            .await?
        else {
            return Ok(None);
        };
        Ok(tags.last().map(|tag| tag.actor_id.clone()))
    }

    pub async fn load(
        big_repo: SharedBigRepo,
        app_doc_id: DocumentId,
        plug_repo: Arc<PlugsRepo>,
        local_user_path: daybook_types::doc::UserPath,
        sql_pool: sqlx::SqlitePool,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let registry = crate::repos::ListenersRegistry::new();
        let store_val = ConfigStore::load(&big_repo, &app_doc_id).await?;
        let local_user_path =
            daybook_types::doc::user_path::for_repo(&local_user_path, "config-repo")?;
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);

        let store = crate::stores::AmStoreHandle::new(
            store_val,
            Arc::clone(&big_repo),
            app_doc_id.clone(),
            local_actor_id.clone(),
        );

        store
            .mutate_sync(|store| {
                store
                    .users
                    .entry(local_actor_id.to_string())
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

        let app_am_handle = big_repo
            .find_doc_handle(&app_doc_id)
            .await?
            .ok_or_eyre("unable to find app doc in am")?;

        let broker = big_repo.ensure_change_broker(app_am_handle.clone()).await?;

        let cancel_token = CancellationToken::new();
        // Register change listener to automatically notify repo listeners
        let (ticket, notif_rx) =
            ConfigStore::register_change_listener(&big_repo, &app_doc_id, vec![]).await?;

        let repo = Self {
            big_repo: Arc::clone(&big_repo),
            app_doc_id: app_doc_id.clone(),
            app_am_handle,
            store,
            registry: Arc::clone(&registry),
            plug_repo,
            local_actor_id,
            local_peer_id: big_repo.local_peer_id(),
            sql_pool,
            cancel_token: cancel_token.clone(),
            sync_config_lock: tokio::sync::Mutex::new(()),
            _change_listener_tickets: vec![ticket],
            _change_broker_leases: vec![broker],
        };
        let repo = Arc::new(repo);

        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let cancel_token = cancel_token.child_token();
            async move {
                repo.notifs_loop(notif_rx, cancel_token)
                    .await
                    .expect("error handling notifs")
            }
        });

        Ok((
            repo,
            crate::repos::RepoStopToken {
                cancel_token,
                worker_handle: Some(worker_handle),
            },
        ))
    }

    async fn notifs_loop(
        &self,
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<
            Vec<am_utils_rs::repo::BigRepoChangeNotification>,
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
                let am_utils_rs::repo::BigRepoChangeNotification::DocChanged {
                    patch,
                    heads,
                    origin,
                    ..
                } = notif
                else {
                    continue;
                };
                let len = events.len();
                self.events_for_patch(
                    &patch,
                    &heads,
                    &mut events,
                    Some(&origin),
                    Some(&self.local_peer_id),
                )
                .await?;
                // events were added
                if len != events.len() {
                    last_heads = Some(ChangeHashSet(Arc::clone(&heads)));
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
                    .big_repo
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
                        store.facet_display_deleted = new_store.facet_display_deleted;
                        store.users = new_store.users;
                        store.users_deleted = new_store.users_deleted;
                        store.mltools = new_store.mltools;
                    })
                    .await?;

                self.registry.notify(events.drain(..));
            }
        }
        Ok(())
    }

    pub async fn upsert_actor_user_path(
        &self,
        actor_id: automerge::ActorId,
        user_path: daybook_types::doc::UserPath,
    ) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let actor_id_str = actor_id.to_string();
        let (_, changed) = self
            .store
            .mutate_sync(move |store| {
                let next = UserMeta {
                    user_path,
                    seen_at: Timestamp::now(),
                };
                if let Some(existing) = store.users.get_mut(&actor_id_str) {
                    existing.replace(self.local_actor_id.clone(), next.into());
                } else {
                    store.users.insert(
                        actor_id_str,
                        Versioned::mint(self.local_actor_id.clone(), next.into()),
                    );
                }
            })
            .await?;
        if changed.is_some() {
            self.registry.notify([ConfigEvent::Changed {
                heads: ChangeHashSet(self.get_config_heads().await?),
                origin: self.local_origin(),
            }]);
        }
        Ok(())
    }

    pub async fn diff_events(
        &self,
        from: ChangeHashSet,
        to: Option<ChangeHashSet>,
    ) -> Res<Vec<ConfigEvent>> {
        let (patches, heads) = self
            .app_am_handle
            .with_document(|am_doc| {
                let heads = if let Some(ref to_set) = to {
                    to_set.clone()
                } else {
                    ChangeHashSet(am_doc.get_heads().into())
                };
                let patches = am_doc
                    .diff_obj(&automerge::ROOT, &from, &heads, true)
                    .expect("diff_obj failed");
                (patches, heads)
            })
            .await?;
        let heads = heads.0;
        let mut events = vec![];
        for patch in patches {
            // Replay path: no live-origin semantics apply.
            self.events_for_patch(&patch, &heads, &mut events, None, None)
                .await?;
        }
        Ok(events)
    }

    pub async fn events_for_init(&self) -> Res<Vec<ConfigEvent>> {
        // Init snapshot is a single "current heads changed" event.
        Ok(vec![ConfigEvent::Changed {
            heads: ChangeHashSet(self.get_config_heads().await?),
            origin: crate::event_origin::SwitchEventOrigin::Bootstrap,
        }])
    }

    async fn events_for_patch(
        &self,
        patch: &automerge::Patch,
        patch_heads: &Arc<[automerge::ChangeHash]>,
        out: &mut Vec<ConfigEvent>,
        live_origin: Option<&am_utils_rs::repo::BigRepoChangeOrigin>,
        exclude_peer_id: Option<&am_utils_rs::repo::PeerId>,
    ) -> Res<()> {
        // Live notification path: local writes are emitted directly by mutators.
        // Historical replay passes `live_origin = None` and must not be skipped.
        if crate::repos::should_skip_live_patch(live_origin, exclude_peer_id) {
            return Ok(());
        }
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
                if !matches!(
                    val,
                    automerge::Value::Scalar(scalar)
                    if matches!(&**scalar, automerge::ScalarValue::Bytes(_))
                ) {
                    return Ok(());
                }
                let vtag = match val {
                    automerge::Value::Scalar(scalar) => match &**scalar {
                        automerge::ScalarValue::Bytes(bytes) => VersionTag::hydrate_bytes(bytes)?,
                        _ => unreachable!("guard above ensures bytes"),
                    },
                    _ => unreachable!("guard above ensures scalar"),
                };
                let event_origin = crate::repos::resolve_origin_from_vtag_actor(
                    &self.local_actor_id,
                    &vtag.actor_id,
                    live_origin,
                );

                if matches!(section_key.as_ref(), "facet_display" | "users" | "mltools") {
                    out.push(ConfigEvent::Changed {
                        heads,
                        origin: event_origin.clone(),
                    });
                }
            }
            automerge::PatchAction::DeleteMap { key } if patch.path.len() == 2 => {
                let Some((_obj, automerge::Prop::Map(section_key))) = patch.path.get(1) else {
                    return Ok(());
                };
                let deleted_prop = match section_key.as_ref() {
                    "facet_display" => Some("facet_display_deleted"),
                    "users" => Some("users_deleted"),
                    _ => None,
                };
                let event_origin = if let Some(deleted_prop) = deleted_prop {
                    let tombstone_actor = self
                        .latest_deleted_actor(deleted_prop, key, patch_heads)
                        .await?;
                    crate::repos::resolve_origin_for_delete(
                        &self.local_actor_id,
                        live_origin,
                        tombstone_actor.as_ref(),
                    )
                } else {
                    self.origin_from_live(live_origin)
                };
                if matches!(
                    section_key.as_ref(),
                    "facet_display"
                        | "users"
                        | "mltools"
                        | "facet_display_deleted"
                        | "users_deleted"
                ) {
                    out.push(ConfigEvent::Changed {
                        heads,
                        origin: event_origin,
                    });
                }
            }
            automerge::PatchAction::PutMap { .. }
            | automerge::PatchAction::PutSeq { .. }
            | automerge::PatchAction::Insert { .. }
                if patch.path.len() >= 2 =>
            {
                let Some((_obj, automerge::Prop::Map(section_key))) = patch.path.get(1) else {
                    return Ok(());
                };
                if matches!(
                    section_key.as_ref(),
                    "facet_display_deleted" | "users_deleted"
                ) {
                    out.push(ConfigEvent::Changed {
                        heads,
                        origin: self.origin_from_live(live_origin),
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn get_config_heads(&self) -> Res<Arc<[automerge::ChangeHash]>> {
        let handle = self
            .big_repo
            .find_doc_handle(&self.app_doc_id)
            .await?
            .ok_or_eyre("app doc not found")?;
        let heads = handle.with_document(|doc| doc.get_heads()).await?;
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
        let (_, changed) = self
            .store
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
        if changed.is_some() {
            self.registry.notify([ConfigEvent::Changed {
                heads: ChangeHashSet(self.get_config_heads().await?),
                origin: self.local_origin(),
            }]);
        }
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

        let (_, changed) = self
            .store
            .mutate_sync(move |store| {
                store
                    .mltools
                    .replace(self.local_actor_id.clone(), config.into());
            })
            .await?;
        if changed.is_some() {
            self.registry.notify([ConfigEvent::Changed {
                heads: ChangeHashSet(self.get_config_heads().await?),
                origin: self.local_origin(),
            }]);
        }
        Ok(())
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
        let _sync_config_guard = self.sync_config_lock.lock().await;
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
        self.registry.notify([ConfigEvent::SyncDevicesChanged {
            origin: self.local_origin(),
        }]);
        Ok(())
    }

    pub async fn remove_known_sync_device(&self, endpoint_id: &iroh::EndpointId) -> Res<bool> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let _sync_config_guard = self.sync_config_lock.lock().await;
        let mut config = crate::app::globals::get_sync_config(&self.sql_pool).await?;
        let before = config.known_devices.len();
        config
            .known_devices
            .retain(|entry| &entry.endpoint_id != endpoint_id);
        let removed = config.known_devices.len() != before;
        if removed {
            crate::app::globals::set_sync_config(&self.sql_pool, &config).await?;
            self.registry.notify([ConfigEvent::SyncDevicesChanged {
                origin: self.local_origin(),
            }]);
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
        let _sync_config_guard = self.sync_config_lock.lock().await;
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
        self.registry.notify([ConfigEvent::SyncDevicesChanged {
            origin: self.local_origin(),
        }]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn upsert_actor_user_path_registers_directory_entries() -> Res<()> {
        let local_user_path = daybook_types::doc::UserPath::from("/test-user/test-device");
        let (big_repo, _acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
            peer_id: crate::peer_id_from_label("test-config-actors"),
            storage: am_utils_rs::repo::StorageConfig::Memory,
        })
        .await?;

        let app_doc = automerge::Automerge::load(&crate::app::version_updates::version_latest()?)?;
        let app_doc_handle = big_repo.add_doc(app_doc).await?;
        let app_doc_id = app_doc_handle.document_id().clone();

        let temp = tempfile::tempdir()?;
        let blobs_repo = crate::blobs::BlobsRepo::new(
            temp.path().join("blobs"),
            local_user_path.to_string(),
            Arc::new(crate::blobs::PartitionStoreMembershipWriter::new(
                big_repo.partition_store(),
            )),
        )
        .await?;
        let (plugs_repo, plugs_stop) = crate::plugs::PlugsRepo::load(
            Arc::clone(&big_repo),
            Arc::clone(&blobs_repo),
            app_doc_id.clone(),
            local_user_path.clone(),
        )
        .await?;
        let sql_ctx = crate::app::SqlCtx::new("sqlite::memory:").await?;
        let (config_repo, config_stop) = ConfigRepo::load(
            Arc::clone(&big_repo),
            app_doc_id,
            plugs_repo,
            local_user_path.clone(),
            sql_ctx.db_pool.clone(),
        )
        .await?;

        for scope in [
            "config-repo",
            "plugs-repo",
            "drawer-repo",
            "dispatch-repo",
            "tables-repo",
            "init-repo",
        ] {
            let scoped_path = daybook_types::doc::user_path::for_repo(&local_user_path, scope)?;
            let scoped_actor = daybook_types::doc::user_path::to_actor_id(&scoped_path);
            config_repo
                .upsert_actor_user_path(scoped_actor.clone(), scoped_path.clone())
                .await?;
            let found = config_repo
                .get_actor_user_path(&scoped_actor)
                .await
                .ok_or_else(|| eyre::eyre!("missing actor mapping for scope {scope}"))?;
            assert_eq!(found, scoped_path);
        }

        config_stop.stop().await?;
        plugs_stop.stop().await?;
        blobs_repo.shutdown().await?;
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
