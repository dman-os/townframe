use crate::interlude::*;

pub mod triage;
pub mod wash_plugin;

pub struct RtConfig {}

pub struct Rt {
    config: RtConfig,
    dispatcher_repo: Arc<DispatcherRepo>,
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize)]
pub struct ActiveDispatch {
    pub deets: ActiveDispatchDeets,
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize)]
pub enum ActiveDispatchDeets {
    PropRoutine(PropRoutineArgs),
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize)]
pub struct PropRoutineArgs {
    pub doc_id: daybook_types::doc::DocId,
    pub heads: ChangeHashSet,
    pub prop_key: String,
}

#[derive(Default, Reconcile, Hydrate)]
pub struct DispatcherStore {
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    pub active_dispatches: HashMap<String, Arc<ActiveDispatch>>,
}

#[async_trait]
impl crate::stores::Store for DispatcherStore {
    const PROP: &str = "dispatcher";
}

pub struct DispatcherRepo {
    // drawer_doc_id: DocumentId,
    acx: AmCtx,
    store: crate::stores::StoreHandle<DispatcherStore>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    _broker: Arc<utils_rs::am::changes::DocChangeBroker>,
}

// Granular event enum for specific changes
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DispatcherEvent {
    ListChanged,
    DispatchChanged { id: String },
    DispatchDeleted { id: String },
}

impl crate::repos::Repo for DispatcherRepo {
    type Event = DispatcherRepo;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }
}

impl DispatcherRepo {
    pub async fn load(acx: AmCtx, app_doc_id: DocumentId) -> Res<Arc<Self>> {
        let registry = crate::repos::ListenersRegistry::new();

        let store = DispatcherStore::load(&acx, &app_doc_id).await?;
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
        DispatcherStore::register_change_listener(&acx, &broker, vec![], {
            move |notifs| notif_tx.send(notifs).expect(ERROR_CHANNEL)
        })
        .await?;

        let repo = Self {
            acx,
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
        // FIXME: this code doesn't seem right and has missing features

        let mut events = vec![];
        while let Some(notifs) = notif_rx.recv().await {
            events.clear();
            for notif in notifs {
                match &notif.patch.action {
                    automerge::PatchAction::PutMap { key, .. } => {
                        // Check if this is a specific item change
                        // Determine which type of item changed based on path
                        if notif.patch.path.len() >= 2 {
                            match &notif.patch.path[1].1 {
                                automerge::Prop::Map(path_key) => match path_key.as_ref() {
                                    "active_dispatches" => events
                                        .push(DispatcherEvent::DispatchChanged { id: key.into() }),
                                    _ => events.push(DispatcherEvent::ListChanged),
                                },
                                _ => events.push(DispatcherEvent::ListChanged),
                            }
                        } else {
                            events.push(DispatcherEvent::ListChanged);
                        }
                    }
                    automerge::PatchAction::DeleteMap { key } => {
                        if notif.patch.path.len() >= 2 {
                            match &notif.patch.path[1].1 {
                                automerge::Prop::Map(path_key) => match path_key.as_ref() {
                                    "active_dispatches" => events
                                        .push(DispatcherEvent::DispatchDeleted { id: key.into() }),
                                    _ => events.push(DispatcherEvent::ListChanged),
                                },
                                _ => events.push(DispatcherEvent::ListChanged),
                            }
                        } else {
                            events.push(DispatcherEvent::ListChanged);
                        }
                    }
                    _ => {
                        // For other operations, send ListChanged
                    }
                }
            }
            for evt in events.drain(..) {
                self.registry.notify(evt);
            }
        }
        Ok(())
    }

    pub async fn get(&self, id: &str) -> Option<Arc<ActiveDispatch>> {
        self.store
            .query_sync(|store| store.active_dispatches.get(id).cloned())
            .await
    }
}
