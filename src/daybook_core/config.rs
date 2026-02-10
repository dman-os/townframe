use crate::interlude::*;

use crate::plugs::{manifest::FacetKeyDisplayHint, PlugsRepo};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct UserMeta {
    #[autosurgeon(with = "utils_rs::am::codecs::path")]
    pub user_path: daybook_types::doc::UserPath,
    #[autosurgeon(with = "utils_rs::am::codecs::date")]
    pub seen_at: Timestamp,
}

#[derive(Reconcile, Hydrate, Clone)]
pub struct ConfigStore {
    pub version: Uuid,
    pub facet_display: HashMap<String, ThroughJson<FacetKeyDisplayHint>>,
    pub users: HashMap<String, ThroughJson<UserMeta>>,
}

impl Default for ConfigStore {
    fn default() -> Self {
        use crate::plugs::manifest::*;

        let mut key_configs = HashMap::new();

        key_configs.insert(
            "created_at".to_string(),
            FacetKeyDisplayHint {
                always_visible: false,
                display_title: Some("Created At".to_string()),
                deets: FacetKeyDisplayDeets::DateTime {
                    display_type: DateTimeFacetDisplayType::Relative,
                },
            }
            .into(),
        );
        key_configs.insert(
            "updated_at".to_string(),
            FacetKeyDisplayHint {
                always_visible: false,
                display_title: Some("Updated At".to_string()),
                deets: FacetKeyDisplayDeets::DateTime {
                    display_type: DateTimeFacetDisplayType::Relative,
                },
            }
            .into(),
        );

        Self {
            version: Uuid::nil(),
            facet_display: key_configs,
            users: HashMap::new(),
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
    Changed { heads: ChangeHashSet },
}

pub struct ConfigRepo {
    acx: AmCtx,
    app_doc_id: DocumentId,
    app_am_handle: samod::DocHandle,
    store: crate::stores::StoreHandle<ConfigStore>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    plug_repo: Arc<PlugsRepo>,
    local_user_path: daybook_types::doc::UserPath,
    local_actor_id: automerge::ActorId,
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
        local_user_path: daybook_types::doc::UserPath,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let registry = crate::repos::ListenersRegistry::new();

        let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);

        let store_val = ConfigStore::load(&acx, &app_doc_id).await?;
        let store = crate::stores::StoreHandle::new(
            store_val,
            acx.clone(),
            app_doc_id.clone(),
            local_actor_id.clone(),
        );

        // Ensure local user path is in the users map
        let actor_id_str = local_actor_id.to_string();
        let current_path = local_user_path.clone();
        store
            .mutate_sync(move |store| {
                store.version = Uuid::new_v4();
                store.users.entry(actor_id_str).or_insert_with(|| {
                    UserMeta {
                        user_path: current_path,
                        seen_at: Timestamp::now(),
                    }
                    .into()
                });
            })
            .await?;

        let app_am_handle = acx
            .find_doc(&app_doc_id)
            .await?
            .ok_or_eyre("unable to find app doc in am")?;

        let (broker, broker_stop) = acx.change_manager().add_doc(app_am_handle.clone()).await?;

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
        &self,
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<
            Vec<utils_rs::am::changes::ChangeNotification>,
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
                last_heads = Some(ChangeHashSet(Arc::clone(&notif.heads)));
                if let Some(actor_id) = utils_rs::am::get_actor_id_from_patch(&notif.patch) {
                    if actor_id == self.local_actor_id {
                        continue;
                    }
                }
                self.events_for_patch(&notif.patch, &notif.heads, &mut events)
                    .await?;
            }

            if !events.is_empty() {
                let heads = last_heads.expect("events not empty");
                let (new_store, _) = self
                    .acx
                    .hydrate_path_at_heads::<ConfigStore>(
                        &self.app_doc_id,
                        &heads,
                        automerge::ROOT,
                        vec![ConfigStore::PROP.into()],
                    )
                    .await?
                    .expect(ERROR_INVALID_PATCH);

                self.store
                    .mutate_sync(|store| {
                        store.version = new_store.version;
                        store.facet_display = new_store.facet_display;
                        store.users = new_store.users;
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
            self.events_for_patch(&patch, &heads, &mut events).await?;
        }
        Ok(events)
    }

    async fn events_for_patch(
        &self,
        patch: &automerge::Patch,
        patch_heads: &Arc<[automerge::ChangeHash]>,
        out: &mut Vec<ConfigEvent>,
    ) -> Res<()> {
        if !utils_rs::am::changes::path_prefix_matches(&[ConfigStore::PROP.into()], &patch.path) {
            return Ok(());
        }

        let heads = ChangeHashSet(Arc::clone(patch_heads));

        match &patch.action {
            automerge::PatchAction::PutMap { key, .. }
                if patch.path.len() == 1 && key == "version" =>
            {
                out.push(ConfigEvent::Changed { heads });
            }
            // For other changes inside the config, also notify
            _ if patch.path.len() > 1 => {
                out.push(ConfigEvent::Changed { heads });
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

    pub async fn get_facet_display_hint(&self, key: String) -> Option<FacetKeyDisplayHint> {
        let hint = self
            .store
            .query_sync(|store| store.facet_display.get(&key).cloned())
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

    pub async fn list_display_hints(&self) -> HashMap<String, FacetKeyDisplayHint> {
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

    pub async fn set_facet_display_hint(&self, key: String, hint: FacetKeyDisplayHint) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        self.store
            .mutate_sync(move |store| {
                store.version = Uuid::new_v4();
                store.facet_display.insert(key, hint.into());
            })
            .await?;
        Ok(())
    }

    pub fn get_local_user_path(&self) -> daybook_types::doc::UserPath {
        self.local_user_path.clone()
    }

    pub fn get_local_actor_id(&self) -> automerge::ActorId {
        self.local_actor_id.clone()
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
                    .map(|doc| doc.0.user_path.clone())
            })
            .await
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
