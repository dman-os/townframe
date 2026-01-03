use crate::interlude::*;

use crate::drawer::DrawerRepo;

use tokio_util::sync::CancellationToken;

pub mod triage;
pub mod wash_plugin;

pub struct RtConfig {}

pub struct Rt {
    _config: RtConfig,
    drawer: Arc<DrawerRepo>,
    _dispatcher_repo: Arc<DispatcherRepo>,
}

#[derive(Debug)]
pub enum DispatchArgs {
    DocInvoke {
        doc_id: String,
        heads: ChangeHashSet,
    },
    DocProp {
        doc_id: String,
        heads: ChangeHashSet,
    },
}

impl Rt {
    pub fn dispatch(
        &self,
        routine: Arc<crate::plugs::manifest::RoutineManifest>,
        args: DispatchArgs,
    ) -> Res<()> {
        use crate::plugs::manifest::RoutineManifestDeets;
        match (&routine.deets, args) {
            (RoutineManifestDeets::DocInvoke {}, DispatchArgs::DocInvoke { doc_id, heads }) => {}
            (
                RoutineManifestDeets::DocProp { working_prop_tag },
                DispatchArgs::DocProp { doc_id, heads },
            ) => {}
            (deets, args) => {
                return Err(ferr!(
                    "routine type and args don't match: {deets:?}, {args:?}"
                ));
            }
        }
        match &routine.r#impl {
            crate::plugs::manifest::RoutineImpl::Wflow { key } => todo!(),
        }
        Ok(())
    }
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
    store: crate::stores::StoreHandle<DispatcherStore>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
    _change_listener_tickets: Vec<utils_rs::am::changes::ChangeListenerRegistration>,
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
    type Event = DispatcherEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }
    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
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
            acx.change_manager().add_doc(handle).await?
        };

        let (notif_tx, notif_rx) = tokio::sync::mpsc::unbounded_channel::<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >();
        let ticket = DispatcherStore::register_change_listener(&acx, &broker, vec![], {
            move |notifs| {
                if let Err(err) = notif_tx.send(notifs) {
                    warn!("failed to send change notifications: {err}");
                }
            }
        })
        .await?;

        let cancel_token = CancellationToken::new();
        let repo = Self {
            store,
            registry: registry.clone(),
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
        events: &mut Vec<DispatcherEvent>,
    ) -> Res<()> {
        events.clear();
        for notif in notifs {
            match &notif.patch.action {
                automerge::PatchAction::PutMap { key, .. } => {
                    // Check if this is a specific item change
                    // Determine which type of item changed based on path
                    if notif.patch.path.len() >= 2 {
                        match &notif.patch.path[1].1 {
                            automerge::Prop::Map(path_key) => match path_key.as_ref() {
                                "active_dispatches" => {
                                    events.push(DispatcherEvent::DispatchChanged { id: key.into() })
                                }
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
                                "active_dispatches" => {
                                    events.push(DispatcherEvent::DispatchDeleted { id: key.into() })
                                }
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
        Ok(())
    }

    pub async fn get(&self, id: &str) -> Option<Arc<ActiveDispatch>> {
        self.store
            .query_sync(|store| store.active_dispatches.get(id).cloned())
            .await
    }
}
