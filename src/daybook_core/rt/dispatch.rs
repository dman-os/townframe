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
        wflow_partition_id: String,
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
    // FUXME: this seems like a bad use of automerge?
    pub wflow_partition_frontier: HashMap<String, u64>,
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
    dispatch_am_handle: samod::DocHandle,
}

// Granular event enum for specific changes
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DispatchEvent {
    ListChanged { heads: ChangeHashSet },
    DispatchAdded { id: String, heads: ChangeHashSet },
    DispatchUpdated { id: String, heads: ChangeHashSet },
    DispatchDeleted { id: String, heads: ChangeHashSet },
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

        let dispatch_am_handle = acx
            .find_doc(&app_doc_id)
            .await?
            .expect("doc should have been loaded");
        let (broker, broker_stop) = {
            acx.change_manager()
                .add_doc(dispatch_am_handle.clone())
                .await?
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
            dispatch_am_handle,
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
        let mut events = vec![];
        loop {
            let notifs = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    while let Ok(notifs) = notif_rx.try_recv() {
                        let mut last_heads = None;
                        for notif in notifs {
                            last_heads = Some(notif.heads.clone());
                            self.events_for_patch(&notif.patch, &notif.heads, &mut events).await?;
                        }
                        if !events.is_empty() {
                            let heads = ChangeHashSet(last_heads.expect("notifs not empty"));
                            self.registry.notify(
                                events
                                    .drain(..)
                                    .chain(std::iter::once(DispatchEvent::ListChanged { heads })),
                            );
                        }
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
            let mut last_heads = None;
            for notif in notifs {
                last_heads = Some(notif.heads.clone());
                self.events_for_patch(&notif.patch, &notif.heads, &mut events)
                    .await?;
            }
            if !events.is_empty() {
                let heads = ChangeHashSet(last_heads.expect("notifs not empty"));
                self.registry.notify(
                    events
                        .drain(..)
                        .chain(std::iter::once(DispatchEvent::ListChanged { heads })),
                );
            }
        }
        Ok(())
    }

    async fn events_for_patch(
        &self,
        patch: &automerge::Patch,
        patch_heads: &Arc<[automerge::ChangeHash]>,
        out: &mut Vec<DispatchEvent>,
    ) -> Res<()> {
        if !utils_rs::am::changes::path_prefix_matches(
            &[DispatchStore::PROP.into(), "active_dispatches".into()],
            &patch.path,
        ) {
            return Ok(());
        }

        let dispatch_heads = ChangeHashSet(patch_heads.clone());

        match &patch.action {
            automerge::PatchAction::PutMap { key, .. } if patch.path.len() == 2 => {
                let exists = self
                    .store
                    .query_sync(|store| store.active_dispatches.contains_key(key))
                    .await;

                out.push(if exists {
                    DispatchEvent::DispatchAdded {
                        id: key.clone(),
                        heads: dispatch_heads,
                    }
                } else {
                    DispatchEvent::DispatchUpdated {
                        id: key.clone(),
                        heads: dispatch_heads,
                    }
                });
            }
            automerge::PatchAction::DeleteMap { key, .. } if patch.path.len() == 2 => {
                out.push(DispatchEvent::DispatchDeleted {
                    id: key.clone(),
                    heads: dispatch_heads,
                });
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn diff_events(
        &self,
        from: ChangeHashSet,
        to: Option<ChangeHashSet>,
    ) -> Res<Vec<DispatchEvent>> {
        let (patches, heads) = self.dispatch_am_handle.with_document(|am_doc| {
            let heads = if let Some(ref to_set) = to {
                to_set.clone()
            } else {
                ChangeHashSet(am_doc.get_heads().into())
            };
            let patches = am_doc.diff(&from, &heads);
            (patches, heads)
        });
        let heads = heads.0;
        let mut events = vec![];
        for patch in patches {
            self.events_for_patch(&patch, &heads, &mut events).await?;
        }
        Ok(events)
    }

    pub async fn get(&self, id: &str) -> Option<Arc<ActiveDispatch>> {
        self.store
            .query_sync(|store| store.active_dispatches.get(id).map(Arc::clone))
            .await
    }

    pub async fn get_wflow_part_frontier(&self, wflow_part_id: &str) -> Option<u64> {
        self.store
            .query_sync(|store| store.wflow_partition_frontier.get(wflow_part_id).cloned())
            .await
    }
    pub async fn set_wflow_part_frontier(&self, wflow_part_id: String, frontier: u64) -> Res<()> {
        self.store
            .mutate_sync(|store| {
                store
                    .wflow_partition_frontier
                    .insert(wflow_part_id, frontier);
            })
            .await?;
        Ok(())
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
        let (old, hash) = self
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
                store.active_dispatches.insert(id.clone(), dispatch)
            })
            .await?;
        assert!(old.is_none(), "fishy");
        let heads = ChangeHashSet(hash.into_iter().collect());
        self.registry.notify([
            DispatchEvent::DispatchAdded {
                id,
                heads: heads.clone(),
            },
            DispatchEvent::ListChanged { heads },
        ]);
        Ok(())
    }

    pub async fn remove(&self, id: String) -> Res<Option<Arc<ActiveDispatch>>> {
        let (old, hash) = self
            .store
            .mutate_sync(|store| store.active_dispatches.remove(&id))
            .await?;
        let heads = ChangeHashSet(hash.into_iter().collect());
        self.registry.notify([
            DispatchEvent::DispatchDeleted {
                id,
                heads: heads.clone(),
            },
            DispatchEvent::ListChanged { heads },
        ]);
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
