use tokio_util::sync::CancellationToken;

use crate::interlude::*;

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub struct ActiveDispatch {
    pub deets: ActiveDispatchDeets,
    pub args: ActiveDispatchArgs,
    #[serde(default = "dispatch_status_active")]
    pub status: DispatchStatus,
    #[serde(default)]
    pub waiting_on_dispatch_ids: Vec<String>,
    #[serde(default)]
    pub on_success_hooks: Vec<DispatchOnSuccessHook>,
}

fn dispatch_status_active() -> DispatchStatus {
    DispatchStatus::Active
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum DispatchStatus {
    Waiting,
    Active,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub enum DispatchOnSuccessHook {
    InitMarkDone {
        init_id: String,
        run_mode: daybook_types::manifest::InitRunMode,
    },
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub enum ActiveDispatchDeets {
    Wflow {
        #[serde(default)]
        wflow_partition_id: Option<String>,
        #[serde(default)]
        entry_id: Option<u64>,
        plug_id: String,
        bundle_name: String,
        wflow_key: String,
        #[serde(default)]
        wflow_job_id: Option<String>,
    },
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub enum ActiveDispatchArgs {
    FacetRoutine(FacetRoutineArgs),
}

#[derive(Hydrate, Reconcile, Serialize, Deserialize, Debug, Clone)]
pub struct FacetRoutineArgs {
    pub doc_id: daybook_types::doc::DocId,
    #[autosurgeon(with = "am_utils_rs::codecs::utf8_path")]
    pub branch_path: daybook_types::doc::BranchPath,
    #[autosurgeon(with = "am_utils_rs::codecs::utf8_path")]
    pub staging_branch_path: daybook_types::doc::BranchPath,
    pub heads: ChangeHashSet,
    pub facet_key: String,
    #[autosurgeon(with = "am_utils_rs::codecs::json")]
    pub facet_acl: Vec<daybook_types::manifest::RoutineFacetAccess>,
    #[autosurgeon(with = "am_utils_rs::codecs::json")]
    pub config_facet_acl: Vec<daybook_types::manifest::RoutineFacetAccess>,
    #[autosurgeon(with = "am_utils_rs::codecs::json")]
    pub local_state_acl: Vec<daybook_types::manifest::RoutineLocalStateAccess>,
}

#[derive(Default, Reconcile, Hydrate)]
pub struct DispatchStore {
    pub dispatches: HashMap<String, Versioned<ThroughJson<Arc<ActiveDispatch>>>>,
    pub active_dispatches: HashMap<String, Versioned<ThroughJson<Arc<ActiveDispatch>>>>,
    pub wflow_to_dispatch: HashMap<String, String>,
    pub cancelled_dispatches: HashMap<String, bool>,
    // FUXME: this seems like a bad use of automerge?
    pub wflow_partition_frontier: HashMap<String, u64>,
}

#[async_trait]
impl crate::stores::AmStore for DispatchStore {
    fn prop() -> Cow<'static, str> {
        "dispatch".into()
    }
}

pub struct DispatchRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,

    big_repo: SharedBigRepo,
    app_doc_id: DocumentId,
    // drawer_doc_id: DocumentId,
    store: crate::stores::AmStoreHandle<DispatchStore>,
    local_actor_id: ActorId,
    local_peer_id: String,
    cancel_token: CancellationToken,
    _change_listener_tickets: Vec<am_utils_rs::repo::BigRepoChangeListenerRegistration>,
    _change_broker_leases: Vec<Arc<am_utils_rs::repo::BigRepoDocChangeBrokerLease>>,
    dispatch_am_handle: samod::DocHandle,
}

// Granular event enum for specific changes
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DispatchEvent {
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
        big_repo: SharedBigRepo,
        app_doc_id: DocumentId,
        local_user_path: daybook_types::doc::UserPath,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let local_user_path =
            daybook_types::doc::user_path::for_repo(&local_user_path, "dispatch-repo")?;
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);
        let registry = crate::repos::ListenersRegistry::new();

        let store_val = DispatchStore::load(&big_repo, &app_doc_id).await?;
        let store = crate::stores::AmStoreHandle::new(
            store_val,
            Arc::clone(&big_repo),
            app_doc_id.clone(),
            local_actor_id.clone(),
        );

        let dispatch_am_handle = big_repo
            .find_doc_handle(&app_doc_id)
            .await?
            .expect("doc should have been loaded");
        let broker = big_repo
            .ensure_change_broker(dispatch_am_handle.clone())
            .await?;

        let cancel_token = CancellationToken::new();
        let (ticket, notif_rx) =
            DispatchStore::register_change_listener(&big_repo, &app_doc_id, vec![]).await?;
        let local_peer_id = big_repo.samod_repo().peer_id().to_string();

        let repo = Self {
            big_repo,
            app_doc_id,
            store,
            registry: Arc::clone(&registry),
            local_actor_id,
            local_peer_id,
            cancel_token: cancel_token.clone(),
            _change_listener_tickets: vec![ticket],
            _change_broker_leases: vec![broker],
            dispatch_am_handle,
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
                let am_utils_rs::repo::BigRepoChangeNotification::DocChanged {
                    patch,
                    heads,
                    origin,
                    ..
                } = notif
                else {
                    continue;
                };
                self.events_for_patch(
                    &patch,
                    &heads,
                    &mut events,
                    Some(&origin),
                    Some(self.local_peer_id.as_str()),
                )
                .await?;
            }

            for event in &events {
                match &event {
                    DispatchEvent::DispatchAdded { id, heads } => {
                        // Hydrate the new dispatch at heads
                        let Some((new_versioned, _)) = self
                            .big_repo
                            .hydrate_path_at_heads::<Versioned<ThroughJson<Arc<ActiveDispatch>>>>(
                                &self.app_doc_id,
                                &heads.0,
                                automerge::ROOT,
                                vec![
                                    DispatchStore::prop().into(),
                                    "dispatches".into(),
                                    autosurgeon::Prop::Key(id.clone().into()),
                                ],
                            )
                            .await?
                        else {
                            warn!(
                                dispatch_id = id,
                                "ignoring stale dispatch patch: entry missing at heads"
                            );
                            continue;
                        };

                        self.store
                            .mutate_sync(|store| {
                                match &new_versioned.deets {
                                    ActiveDispatchDeets::Wflow { wflow_job_id, .. } => {
                                        if let Some(wflow_job_id) = wflow_job_id {
                                            store
                                                .wflow_to_dispatch
                                                .insert(wflow_job_id.clone(), id.clone());
                                        }
                                    }
                                }
                                store.dispatches.insert(id.clone(), new_versioned.clone());
                                if new_versioned.status == DispatchStatus::Active {
                                    store.active_dispatches.insert(id.clone(), new_versioned);
                                }
                            })
                            .await?;
                    }
                    DispatchEvent::DispatchUpdated { id, heads } => {
                        let Some((new_versioned, _)) = self
                            .big_repo
                            .hydrate_path_at_heads::<Versioned<ThroughJson<Arc<ActiveDispatch>>>>(
                                &self.app_doc_id,
                                &heads.0,
                                automerge::ROOT,
                                vec![
                                    DispatchStore::prop().into(),
                                    "dispatches".into(),
                                    autosurgeon::Prop::Key(id.clone().into()),
                                ],
                            )
                            .await?
                        else {
                            continue;
                        };
                        self.store
                            .mutate_sync(|store| {
                                if let Some(old) =
                                    store.dispatches.insert(id.clone(), new_versioned.clone())
                                {
                                    if let ActiveDispatchDeets::Wflow {
                                        wflow_job_id: Some(job),
                                        ..
                                    } = &old.deets
                                    {
                                        store.wflow_to_dispatch.remove(job);
                                    }
                                }
                                if let ActiveDispatchDeets::Wflow {
                                    wflow_job_id: Some(job),
                                    ..
                                } = &new_versioned.deets
                                {
                                    store.wflow_to_dispatch.insert(job.clone(), id.clone());
                                }
                                match new_versioned.status {
                                    DispatchStatus::Active => {
                                        store.active_dispatches.insert(id.clone(), new_versioned);
                                    }
                                    _ => {
                                        store.active_dispatches.remove(id);
                                    }
                                }
                            })
                            .await?;
                    }
                    DispatchEvent::DispatchDeleted { id, .. } => {
                        self.store
                            .mutate_sync(|store| {
                                store.dispatches.remove(id);
                                if let Some(old_dispatch) = store.active_dispatches.remove(id) {
                                    if let ActiveDispatchDeets::Wflow {
                                        wflow_job_id: Some(wflow_job_id),
                                        ..
                                    } = &old_dispatch.deets
                                    {
                                        store.wflow_to_dispatch.remove(wflow_job_id);
                                    }
                                }
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
        origin: Option<&am_utils_rs::repo::BigRepoChangeOrigin>,
        exclude_peer: Option<&str>,
    ) -> Res<()> {
        if let Some(origin) = origin {
            match origin {
                am_utils_rs::repo::BigRepoChangeOrigin::Local => return Ok(()),
                am_utils_rs::repo::BigRepoChangeOrigin::Remote { peer_id, .. } => {
                    if let Some(exclude_peer) = exclude_peer {
                        if peer_id.to_string() == exclude_peer {
                            return Ok(());
                        }
                    }
                }
                am_utils_rs::repo::BigRepoChangeOrigin::Bootstrap => {}
            }
        }
        if !am_utils_rs::repo::big_repo_path_prefix_matches(
            &[DispatchStore::prop().into(), "dispatches".into()],
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
            } if patch.path.len() == 3 && key == "vtag" => {
                let Some((_obj, automerge::Prop::Map(dispatch_id))) = patch.path.get(2) else {
                    return Ok(());
                };

                let vtag_bytes = match val {
                    automerge::Value::Scalar(scalar) => match &**scalar {
                        automerge::ScalarValue::Bytes(bytes) => bytes,
                        _ => return Ok(()),
                    },
                    _ => return Ok(()),
                };
                let vtag = VersionTag::hydrate_bytes(vtag_bytes)?;
                out.push(if vtag.version.is_nil() {
                    DispatchEvent::DispatchAdded {
                        id: dispatch_id.clone(),
                        heads: dispatch_heads,
                    }
                } else {
                    DispatchEvent::DispatchUpdated {
                        id: dispatch_id.clone(),
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
            let patches = am_doc
                .diff_obj(&automerge::ROOT, &from, &heads, true)
                .expect("diff_obj failed");
            (patches, heads)
        });
        let heads = heads.0;
        let mut events = vec![];
        for patch in patches {
            self.events_for_patch(&patch, &heads, &mut events, None, None)
                .await?;
        }
        Ok(events)
    }

    pub async fn events_for_init(&self) -> Res<Vec<DispatchEvent>> {
        let heads = self.get_dispatch_heads();
        let dispatch_ids = self
            .store
            .query_sync(|store| store.dispatches.keys().cloned().collect::<Vec<_>>())
            .await;
        let mut events = Vec::with_capacity(dispatch_ids.len());
        for id in dispatch_ids {
            events.push(DispatchEvent::DispatchAdded {
                id,
                heads: heads.clone(),
            });
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
                    .dispatches
                    .get(id)
                    .map(|versioned| Arc::clone(&versioned.val.0))
            })
            .await
    }

    pub async fn get_active(&self, id: &str) -> Option<Arc<ActiveDispatch>> {
        self.store
            .query_sync(|store| {
                let dispatch = store.dispatches.get(id)?;
                if dispatch.status != DispatchStatus::Active {
                    return None;
                }
                Some(Arc::clone(&dispatch.val.0))
            })
            .await
    }

    pub async fn get_any(&self, id: &str) -> Option<Arc<ActiveDispatch>> {
        self.get(id).await
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
                store.dispatches.get(disp_id).and_then(|versioned| {
                    if versioned.status == DispatchStatus::Active {
                        Some(Arc::clone(&versioned.val.0))
                    } else {
                        None
                    }
                })
            })
            .await
    }

    pub async fn add(&self, id: String, dispatch: Arc<ActiveDispatch>) -> Res<()> {
        debug!(?id, "adding dispatch to repo");
        let (_, hash) = self
            .store
            .mutate_sync(|store| {
                let versioned = {
                    let versioned: Versioned<ThroughJson<Arc<ActiveDispatch>>> =
                        Versioned::mint(self.local_actor_id.clone(), Arc::clone(&dispatch).into());
                    versioned
                };

                if let ActiveDispatchDeets::Wflow {
                    wflow_job_id: Some(wflow_job_id),
                    ..
                } = &dispatch.deets
                {
                    let old = store
                        .wflow_to_dispatch
                        .insert(wflow_job_id.clone(), id.clone());
                    assert!(old.is_none(), "fishy");
                }
                store.cancelled_dispatches.remove(&id);
                store.dispatches.insert(id.clone(), versioned.clone());
                if dispatch.status == DispatchStatus::Active {
                    store.active_dispatches.insert(id.clone(), versioned);
                }
            })
            .await?;
        let heads = ChangeHashSet(hash.into_iter().collect());
        self.registry.notify([DispatchEvent::DispatchAdded {
            id,
            heads: heads.clone(),
        }]);
        Ok(())
    }

    pub async fn complete(
        &self,
        id: String,
        status: DispatchStatus,
    ) -> Res<Option<Arc<ActiveDispatch>>> {
        assert!(matches!(
            status,
            DispatchStatus::Succeeded | DispatchStatus::Failed | DispatchStatus::Cancelled
        ));
        let (old, hash) = self
            .store
            .mutate_sync(|store| {
                let old = store.dispatches.get(&id).cloned();
                store.cancelled_dispatches.remove(&id);
                if let Some(old_dispatch) = old.as_ref() {
                    let old_dispatch = &old_dispatch.val.0;
                    if let ActiveDispatchDeets::Wflow {
                        wflow_job_id: Some(wflow_job_id),
                        ..
                    } = &old_dispatch.deets
                    {
                        store.wflow_to_dispatch.remove(wflow_job_id);
                    }
                }
                store.active_dispatches.remove(&id);
                if let Some(old_dispatch) = old.as_ref() {
                    let mut next = (*old_dispatch.val.0).clone();
                    next.status = status;
                    let versioned: Versioned<ThroughJson<Arc<ActiveDispatch>>> =
                        Versioned::update(self.local_actor_id.clone(), Arc::new(next).into());
                    store.dispatches.insert(id.clone(), versioned);
                }
                old
            })
            .await?;
        let heads = ChangeHashSet(hash.into_iter().collect());
        self.registry.notify([DispatchEvent::DispatchUpdated {
            id,
            heads: heads.clone(),
        }]);
        Ok(old.map(|disp| Arc::clone(&disp.val.0)))
    }

    pub async fn list(&self) -> Vec<(String, Arc<ActiveDispatch>)> {
        self.store
            .query_sync(|store| {
                store
                    .active_dispatches
                    .iter()
                    .map(|(key, item)| (key.clone(), Arc::clone(&item.val.0)))
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
            .query_sync(|store| {
                store.dispatches.get(&id).is_some_and(|dispatch| {
                    matches!(
                        dispatch.status,
                        DispatchStatus::Active | DispatchStatus::Waiting
                    )
                })
            })
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

    pub async fn list_waiting_on(
        &self,
        dependency_dispatch_id: &str,
    ) -> Vec<(String, Arc<ActiveDispatch>)> {
        let dependency_dispatch_id = dependency_dispatch_id.to_string();
        self.store
            .query_sync(move |store| {
                store
                    .dispatches
                    .iter()
                    .filter_map(|(id, dispatch)| {
                        if dispatch.status == DispatchStatus::Waiting
                            && dispatch
                                .waiting_on_dispatch_ids
                                .iter()
                                .any(|dep| dep == &dependency_dispatch_id)
                        {
                            Some((id.clone(), Arc::clone(&dispatch.val.0)))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .await
    }

    pub async fn remove_waiting_dependency(
        &self,
        dispatch_id: &str,
        dependency_dispatch_id: &str,
    ) -> Res<Option<Arc<ActiveDispatch>>> {
        let dispatch_id = dispatch_id.to_string();
        let dependency_dispatch_id = dependency_dispatch_id.to_string();
        let (next, _hash) = self
            .store
            .mutate_sync(|store| {
                let cur = store.dispatches.get(&dispatch_id).cloned()?;
                if cur.status != DispatchStatus::Waiting {
                    return None;
                }
                let mut updated = (*cur.val.0).clone();
                updated
                    .waiting_on_dispatch_ids
                    .retain(|dep| dep != &dependency_dispatch_id);
                let ready = updated.waiting_on_dispatch_ids.is_empty();
                let versioned: Versioned<ThroughJson<Arc<ActiveDispatch>>> = Versioned::update(
                    self.local_actor_id.clone(),
                    Arc::new(updated.clone()).into(),
                );
                store.dispatches.insert(dispatch_id.clone(), versioned);
                if ready {
                    Some(Arc::new(updated))
                } else {
                    None
                }
            })
            .await?;
        Ok(next)
    }

    pub async fn activate_waiting(
        &self,
        dispatch_id: &str,
        deets: ActiveDispatchDeets,
    ) -> Res<Arc<ActiveDispatch>> {
        let dispatch_id = dispatch_id.to_string();
        let (next, hash) = self
            .store
            .try_mutate_sync(|store| {
                let Some(cur) = store.dispatches.get(&dispatch_id).cloned() else {
                    eyre::bail!("dispatch not found under {dispatch_id}");
                };
                if cur.status != DispatchStatus::Waiting {
                    eyre::bail!("dispatch is not waiting: {dispatch_id}");
                }
                if !cur.waiting_on_dispatch_ids.is_empty() {
                    eyre::bail!("dispatch still has unresolved dependencies: {dispatch_id}");
                }
                let mut updated = (*cur.val.0).clone();
                updated.status = DispatchStatus::Active;
                updated.deets = deets;
                let arc = Arc::new(updated.clone());
                let versioned: Versioned<ThroughJson<Arc<ActiveDispatch>>> =
                    Versioned::update(self.local_actor_id.clone(), Arc::clone(&arc).into());
                store
                    .dispatches
                    .insert(dispatch_id.clone(), versioned.clone());
                store
                    .active_dispatches
                    .insert(dispatch_id.clone(), versioned);
                if let ActiveDispatchDeets::Wflow {
                    wflow_job_id: Some(job),
                    ..
                } = &updated.deets
                {
                    store
                        .wflow_to_dispatch
                        .insert(job.clone(), dispatch_id.clone());
                }
                Ok(arc)
            })
            .await?;
        let heads = ChangeHashSet(hash.into_iter().collect());
        self.registry.notify([DispatchEvent::DispatchUpdated {
            id: dispatch_id,
            heads,
        }]);
        Ok(next)
    }

    pub async fn update_active_deets(
        &self,
        dispatch_id: &str,
        deets: ActiveDispatchDeets,
    ) -> Res<Arc<ActiveDispatch>> {
        let dispatch_id = dispatch_id.to_string();
        let (next, hash) = self
            .store
            .try_mutate_sync(|store| {
                let Some(cur) = store.dispatches.get(&dispatch_id).cloned() else {
                    eyre::bail!("dispatch not found under {dispatch_id}");
                };
                if cur.status != DispatchStatus::Active {
                    eyre::bail!("dispatch is not active: {dispatch_id}");
                }
                let mut updated = (*cur.val.0).clone();
                if let ActiveDispatchDeets::Wflow {
                    wflow_job_id: Some(job),
                    ..
                } = &updated.deets
                {
                    store.wflow_to_dispatch.remove(job);
                }
                updated.deets = deets;
                let arc = Arc::new(updated.clone());
                let versioned: Versioned<ThroughJson<Arc<ActiveDispatch>>> =
                    Versioned::update(self.local_actor_id.clone(), Arc::clone(&arc).into());
                store
                    .dispatches
                    .insert(dispatch_id.clone(), versioned.clone());
                store
                    .active_dispatches
                    .insert(dispatch_id.clone(), versioned);
                if let ActiveDispatchDeets::Wflow {
                    wflow_job_id: Some(job),
                    ..
                } = &updated.deets
                {
                    store
                        .wflow_to_dispatch
                        .insert(job.clone(), dispatch_id.clone());
                }
                Ok(arc)
            })
            .await?;
        let heads = ChangeHashSet(hash.into_iter().collect());
        self.registry.notify([DispatchEvent::DispatchUpdated {
            id: dispatch_id,
            heads,
        }]);
        Ok(next)
    }

    pub async fn set_waiting_failed(&self, dispatch_id: &str) -> Res<()> {
        let dispatch_id = dispatch_id.to_string();
        let (_, hash) = self
            .store
            .mutate_sync(|store| {
                if let Some(cur) = store.dispatches.get(&dispatch_id).cloned() {
                    if let ActiveDispatchDeets::Wflow {
                        wflow_job_id: Some(wflow_job_id),
                        ..
                    } = &cur.val.0.deets
                    {
                        store.wflow_to_dispatch.remove(wflow_job_id);
                    }
                    store.cancelled_dispatches.remove(&dispatch_id);
                    let mut updated = (*cur.val.0).clone();
                    updated.status = DispatchStatus::Failed;
                    let versioned: Versioned<ThroughJson<Arc<ActiveDispatch>>> =
                        Versioned::update(self.local_actor_id.clone(), Arc::new(updated).into());
                    store.dispatches.insert(dispatch_id.clone(), versioned);
                }
            })
            .await?;
        let heads = ChangeHashSet(hash.into_iter().collect());
        self.registry.notify([DispatchEvent::DispatchUpdated {
            id: dispatch_id,
            heads,
        }]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::version_updates;
    use crate::repos::{Repo, SubscribeOpts, TryRecvError};

    async fn setup_repo() -> Res<(Arc<DispatchRepo>, tempfile::TempDir)> {
        let local_user_path = daybook_types::doc::UserPath::from("/test-user/test-device");
        let (big_repo, _acx_stop) = BigRepo::boot(am_utils_rs::repo::Config {
            peer_id: "test-dispatch".into(),
            storage: am_utils_rs::repo::StorageConfig::Memory,
        })
        .await?;
        let doc = automerge::Automerge::load(&version_updates::version_latest()?)?;
        let handle = big_repo.add_doc(doc).await?;
        let doc_id = handle.document_id().clone();
        let (repo, _stop) = DispatchRepo::load(big_repo, doc_id, local_user_path).await?;
        Ok((repo, tempfile::tempdir()?))
    }

    fn mock_dispatch(job_id: &str) -> Arc<ActiveDispatch> {
        Arc::new(ActiveDispatch {
            deets: ActiveDispatchDeets::Wflow {
                wflow_partition_id: Some("part-1".to_string()),
                entry_id: Some(1),
                plug_id: "@test/plug".to_string(),
                bundle_name: "bundle".to_string(),
                wflow_key: "key".to_string(),
                wflow_job_id: Some(job_id.to_string()),
            },
            args: ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                doc_id: "doc1".to_string(),
                branch_path: "main".into(),
                staging_branch_path: "@daybook/wip/staging".into(),
                heads: ChangeHashSet(default()),
                facet_key: "facet".to_string(),
                facet_acl: vec![],
                config_facet_acl: vec![],
                local_state_acl: vec![],
            }),
            status: DispatchStatus::Active,
            waiting_on_dispatch_ids: vec![],
            on_success_hooks: vec![],
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_emits_single_local_dispatch_added_event() -> Res<()> {
        let (repo, _temp) = setup_repo().await?;
        let listener = repo.subscribe(SubscribeOpts::new(16));

        repo.add("disp-1".to_string(), mock_dispatch("job-1"))
            .await?;

        let first: Arc<DispatchEvent> = listener
            .recv_async()
            .await
            .map_err(|err| ferr!("listener recv failed: {err:?}"))?;
        assert!(
            matches!(&*first, DispatchEvent::DispatchAdded { id, .. } if id == "disp-1"),
            "expected DispatchAdded event, got: {first:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            matches!(listener.try_recv(), Err(TryRecvError::Empty)),
            "expected no duplicate local dispatch event"
        );
        Ok(())
    }
}
