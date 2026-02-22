use tokio_util::sync::CancellationToken;

use crate::interlude::*;

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub struct ActiveDispatch {
    pub deets: ActiveDispatchDeets,
    pub args: ActiveDispatchArgs,
}

#[derive(Debug, Clone, Reconcile, Hydrate, Serialize, Deserialize)]
pub struct VersionedDispatch {
    pub version: Uuid,
    #[autosurgeon(with = "utils_rs::am::codecs::json")]
    pub payload: Arc<ActiveDispatch>,
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
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

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub enum ActiveDispatchArgs {
    FacetRoutine(FacetRoutineArgs),
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub struct FacetRoutineArgs {
    pub doc_id: daybook_types::doc::DocId,
    #[autosurgeon(with = "utils_rs::am::codecs::path")]
    pub branch_path: daybook_types::doc::BranchPath,
    #[autosurgeon(with = "utils_rs::am::codecs::path")]
    pub staging_branch_path: daybook_types::doc::BranchPath,
    pub heads: ChangeHashSet,
    pub facet_key: String,
    #[autosurgeon(with = "utils_rs::am::codecs::json")]
    pub facet_acl: Vec<crate::plugs::manifest::RoutineFacetAccess>,
    #[autosurgeon(with = "utils_rs::am::codecs::json")]
    pub local_state_acl: Vec<crate::plugs::manifest::RoutineLocalStateAccess>,
}

#[derive(Default, Reconcile, Hydrate)]
pub struct DispatchStore {
    pub active_dispatches: HashMap<String, VersionedDispatch>,
    pub wflow_to_dispatch: HashMap<String, String>,
    pub cancelled_dispatches: HashMap<String, bool>,
    // FUXME: this seems like a bad use of automerge?
    pub wflow_partition_frontier: HashMap<String, u64>,
}

#[async_trait]
impl crate::stores::Store for DispatchStore {
    fn prop() -> Cow<'static, str> {
        "dispatch".into()
    }
}

pub struct DispatchRepo {
    pub acx: AmCtx,
    pub app_doc_id: DocumentId,
    // drawer_doc_id: DocumentId,
    store: crate::stores::StoreHandle<DispatchStore>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    pub local_actor_id: automerge::ActorId,
    cancel_token: CancellationToken,
    _change_listener_tickets: Vec<utils_rs::am::changes::ChangeListenerRegistration>,
    dispatch_am_handle: samod::DocHandle,
}

// Granular event enum for specific changes
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DispatchEvent {
    DispatchAdded { id: String, heads: ChangeHashSet },
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
        local_actor_id: automerge::ActorId,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let registry = crate::repos::ListenersRegistry::new();

        let store_val = DispatchStore::load(&acx, &app_doc_id).await?;
        let store = crate::stores::StoreHandle::new(
            store_val,
            acx.clone(),
            app_doc_id.clone(),
            local_actor_id.clone(),
        );

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
            acx,
            app_doc_id,
            store,
            registry: Arc::clone(&registry),
            local_actor_id,
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
        &self,
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

            for notif in notifs {
                if notif.is_local_only(&self.local_actor_id) {
                    continue;
                }

                self.events_for_patch(&notif.patch, &notif.heads, &mut events)
                    .await?;
            }

            for event in &events {
                match &event {
                    DispatchEvent::DispatchAdded { id, heads } => {
                        // Hydrate the new dispatch at heads
                        let (new_versioned, _) = self
                            .acx
                            .hydrate_path_at_heads::<VersionedDispatch>(
                                &self.app_doc_id,
                                &heads.0,
                                automerge::ROOT,
                                vec![
                                    DispatchStore::prop().into(),
                                    "active_dispatches".into(),
                                    autosurgeon::Prop::Key(id.clone().into()),
                                ],
                            )
                            .await?
                            .expect(ERROR_INVALID_PATCH);

                        self.store
                            .mutate_sync(|store| {
                                store.active_dispatches.insert(id.clone(), new_versioned);
                            })
                            .await?;
                    }
                    DispatchEvent::DispatchDeleted { id, .. } => {
                        self.store
                            .mutate_sync(|store| {
                                store.active_dispatches.remove(id);
                            })
                            .await?;
                    }
                }
            }

            if !events.is_empty() {
                self.registry.notify(events.drain(..));
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
            &[DispatchStore::prop().into(), "active_dispatches".into()],
            &patch.path,
        ) {
            return Ok(());
        }

        let dispatch_heads = ChangeHashSet(Arc::clone(patch_heads));

        match &patch.action {
            automerge::PatchAction::PutMap {
                key,
                value: (val, _obj_id),
                ..
            } if patch.path.len() == 3 && key == "version" => {
                let Some((_obj, automerge::Prop::Map(dispatch_id))) = patch.path.get(2) else {
                    return Ok(());
                };

                let version_bytes = match val {
                    automerge::Value::Scalar(scalar) => match &**scalar {
                        automerge::ScalarValue::Bytes(bytes) => bytes,
                        _ => return Ok(()),
                    },
                    _ => return Ok(()),
                };
                let version = Uuid::from_slice(version_bytes)?;

                out.push(if version.is_nil() {
                    DispatchEvent::DispatchAdded {
                        id: dispatch_id.clone(),
                        heads: dispatch_heads,
                    }
                } else {
                    panic!("dispatch update detected")
                    // DispatchEvent::DispatchUpdated {
                    //     id: dispatch_id.clone(),
                    //     heads: dispatch_heads,
                    // }
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

    pub fn get_dispatch_heads(&self) -> ChangeHashSet {
        self.dispatch_am_handle
            .with_document(|am_doc| ChangeHashSet(am_doc.get_heads().into()))
    }

    pub async fn get(&self, id: &str) -> Option<Arc<ActiveDispatch>> {
        self.store
            .query_sync(|store| {
                store
                    .active_dispatches
                    .get(id)
                    .map(|versioned| Arc::clone(&versioned.payload))
            })
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
                store
                    .active_dispatches
                    .get(disp_id)
                    .map(|versioned| Arc::clone(&versioned.payload))
            })
            .await
    }

    pub async fn add(&self, id: String, dispatch: Arc<ActiveDispatch>) -> Res<()> {
        debug!(?id, "adding dispatch to repo");
        let (_, hash) = self
            .store
            .mutate_sync(|store| {
                let versioned = VersionedDispatch {
                    version: Uuid::nil(),
                    payload: Arc::clone(&dispatch),
                };

                match &dispatch.deets {
                    ActiveDispatchDeets::Wflow { wflow_job_id, .. } => {
                        let old = store
                            .wflow_to_dispatch
                            .insert(wflow_job_id.clone(), id.clone());
                        assert!(old.is_none(), "fishy");
                    }
                }
                store.cancelled_dispatches.remove(&id);
                store.active_dispatches.insert(id.clone(), versioned);
            })
            .await?;
        let heads = ChangeHashSet(hash.into_iter().collect());
        self.registry.notify([DispatchEvent::DispatchAdded {
            id,
            heads: heads.clone(),
        }]);
        Ok(())
    }

    pub async fn remove(&self, id: String) -> Res<Option<Arc<ActiveDispatch>>> {
        let (old, hash) = self
            .store
            .mutate_sync(|store| {
                let old = store.active_dispatches.remove(&id);
                store.cancelled_dispatches.remove(&id);
                if let Some(old_dispatch) = &old {
                    match &old_dispatch.payload.deets {
                        ActiveDispatchDeets::Wflow { wflow_job_id, .. } => {
                            store.wflow_to_dispatch.remove(wflow_job_id);
                        }
                    }
                }
                old
            })
            .await?;
        let heads = ChangeHashSet(hash.into_iter().collect());
        self.registry.notify([DispatchEvent::DispatchDeleted {
            id,
            heads: heads.clone(),
        }]);
        Ok(old.map(|disp| disp.payload))
    }

    pub async fn list(&self) -> Vec<(String, Arc<ActiveDispatch>)> {
        self.store
            .query_sync(|store| {
                store
                    .active_dispatches
                    .iter()
                    .map(|(key, item)| (key.clone(), Arc::clone(&item.payload)))
                    .collect()
            })
            .await
    }

    /// Marks a dispatch as cancellation-requested.
    /// Returns true when this call performed the first mark; false if it was already marked.
    pub async fn mark_cancelled(&self, id: &str) -> Res<bool> {
        let id = id.to_string();
        let exists = self
            .store
            .query_sync(|store| store.active_dispatches.contains_key(&id))
            .await;
        if !exists {
            eyre::bail!("dispatch not found under {id}");
        }

        let (marked_now, _hash) = self
            .store
            .mutate_sync(|store| {
                store
                    .cancelled_dispatches
                    .insert(id.clone(), true)
                    .is_none()
            })
            .await?;
        Ok(marked_now)
    }
}
