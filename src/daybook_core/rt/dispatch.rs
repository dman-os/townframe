use tokio_util::sync::CancellationToken;

use crate::interlude::*;

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug)]
pub struct ActiveDispatch {
    pub deets: ActiveDispatchDeets,
    pub args: ActiveDispatchArgs,
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug)]
pub enum ActiveDispatchDeets {
    Wflow {
        entry_id: u64,
        plug_id: String,
        bundle_name: String,
        wflow_key: String,
        wflow_job_id: String,
    },
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug)]
pub enum ActiveDispatchArgs {
    PropRoutine(PropRoutineArgs),
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug)]
pub struct PropRoutineArgs {
    pub doc_id: daybook_types::doc::DocId,
    pub branch_name: String,
    pub heads: ChangeHashSet,
    pub prop_key: String,
}

#[derive(Default, Reconcile, Hydrate)]
pub struct DispatchStore {
    pub active_dispatches: HashMap<String, Arc<ActiveDispatch>>,
    pub wflow_to_dispatch: HashMap<String, String>,
}

#[async_trait]
impl crate::stores::Store for DispatchStore {
    const PROP: &str = "dispatch";
}

pub struct DispatchRepo {
    // drawer_doc_id: DocumentId,
    store: crate::stores::StoreHandle<DispatchStore>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
    _change_listener_tickets: Vec<utils_rs::am::changes::ChangeListenerRegistration>,
}

// Granular event enum for specific changes
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DispatchEvent {
    ListChanged,
    DispatchChanged { id: String },
    DispatchDeleted { id: String },
}

impl crate::repos::Repo for DispatchRepo {
    type Event = DispatchEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }
    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

impl DispatchRepo {
    pub async fn load(
        acx: AmCtx,
        app_doc_id: DocumentId,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let registry = crate::repos::ListenersRegistry::new();

        let store = DispatchStore::load(&acx, &app_doc_id).await?;
        let store = crate::stores::StoreHandle::new(store, acx.clone(), app_doc_id.clone());

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
        let ticket = DispatchStore::register_change_listener(&acx, &broker, vec![], {
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
            registry: Arc::clone(&registry),
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
        events: &mut Vec<DispatchEvent>,
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
                                    events.push(DispatchEvent::DispatchChanged { id: key.into() })
                                }
                                _ => events.push(DispatchEvent::ListChanged),
                            },
                            _ => events.push(DispatchEvent::ListChanged),
                        }
                    } else {
                        events.push(DispatchEvent::ListChanged);
                    }
                }
                automerge::PatchAction::DeleteMap { key } => {
                    if notif.patch.path.len() >= 2 {
                        match &notif.patch.path[1].1 {
                            automerge::Prop::Map(path_key) => match path_key.as_ref() {
                                "active_dispatches" => {
                                    events.push(DispatchEvent::DispatchDeleted { id: key.into() })
                                }
                                _ => events.push(DispatchEvent::ListChanged),
                            },
                            _ => events.push(DispatchEvent::ListChanged),
                        }
                    } else {
                        events.push(DispatchEvent::ListChanged);
                    }
                }
                _ => {
                    // For other operations, send ListChanged
                }
            }
        }
        self.registry.notify(events.drain(..));
        Ok(())
    }

    pub async fn get(&self, id: &str) -> Option<Arc<ActiveDispatch>> {
        self.store
            .query_sync(|store| store.active_dispatches.get(id).map(Arc::clone))
            .await
    }

    pub async fn get_by_wflow_job(&self, job_id: &str) -> Option<Arc<ActiveDispatch>> {
        self.store
            .query_sync(|store| {
                let disp_id = store.wflow_to_dispatch.get(job_id)?;
                store.active_dispatches.get(disp_id).map(Arc::clone)
            })
            .await
    }

    pub async fn add(&self, id: String, dispatch: Arc<ActiveDispatch>) -> Res<()> {
        debug!(?id, "adding dispatch to repo");
        let (old, commit) = self
            .store
            .mutate_sync(|store| {
                match &dispatch.deets {
                    ActiveDispatchDeets::Wflow { wflow_job_id, .. } => {
                        let old = store
                            .wflow_to_dispatch
                            .insert(wflow_job_id.clone(), id.clone());
                        assert!(old.is_none(), "fishy");
                    }
                }
                store.active_dispatches.insert(id, dispatch)
            })
            .await?;
        debug!(?commit, "dispatch added to repo");
        assert!(old.is_none(), "fishy");
        Ok(())
    }

    pub async fn remove(&self, id: &str) -> Res<Option<Arc<ActiveDispatch>>> {
        let (old, _hash) = self
            .store
            .mutate_sync(|store| store.active_dispatches.remove(id))
            .await?;
        Ok(old)
    }

    pub async fn list(&self) -> Vec<(String, Arc<ActiveDispatch>)> {
        self.store
            .query_sync(|store| {
                store
                    .active_dispatches
                    .iter()
                    .map(|(key, item)| (key.clone(), Arc::clone(&item)))
                    .collect()
            })
            .await
    }
}
