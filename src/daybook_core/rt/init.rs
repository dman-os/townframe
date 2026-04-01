use crate::interlude::*;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerInstallDeets {
    pub completed_at: String,
    pub completed_by_actor_id: String,
}

#[derive(Reconcile, Hydrate)]
pub struct InitStore {
    pub per_install_done: HashMap<String, Versioned<ThroughJson<PerInstallDeets>>>,
}

impl Default for InitStore {
    fn default() -> Self {
        Self {
            per_install_done: default(),
        }
    }
}

#[async_trait]
impl crate::stores::AmStore for InitStore {
    fn prop() -> Cow<'static, str> {
        "init".into()
    }
}

#[derive(Debug, Clone)]
pub enum InitEvent {
    Changed { heads: ChangeHashSet },
}

pub struct InitRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,
    big_repo: SharedBigRepo,
    app_doc_id: DocumentId,
    app_am_handle: samod::DocHandle,
    store: crate::stores::AmStoreHandle<InitStore>,
    local_actor_id: ActorId,
    sql_pool: sqlx::SqlitePool,
    running_dispatches: tokio::sync::RwLock<HashMap<String, String>>,
    per_boot_done: tokio::sync::RwLock<HashSet<String>>,
    cancel_token: CancellationToken,
    local_peer_id: String,
    _change_listener_tickets: Vec<am_utils_rs::repo::BigRepoChangeListenerRegistration>,
    _change_broker_leases: Vec<Arc<am_utils_rs::repo::BigRepoDocChangeBrokerLease>>,
}

impl crate::repos::Repo for InitRepo {
    type Event = InitEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }
    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

impl InitRepo {
    pub async fn load(
        big_repo: SharedBigRepo,
        app_doc_id: DocumentId,
        local_actor_id: ActorId,
        sql_pool: sqlx::SqlitePool,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS init_per_node (
                init_id TEXT PRIMARY KEY,
                completed_at TEXT NOT NULL
            )
            "#,
        )
        .execute(&sql_pool)
        .await?;

        let registry = crate::repos::ListenersRegistry::new();
        let store_val = InitStore::load(&big_repo, &app_doc_id).await?;
        let store = crate::stores::AmStoreHandle::new(
            store_val,
            Arc::clone(&big_repo),
            app_doc_id.clone(),
            local_actor_id.clone(),
        );

        let app_am_handle = big_repo
            .find_doc_handle(&app_doc_id)
            .await?
            .ok_or_eyre("unable to find app doc in am")?;
        let broker = big_repo.ensure_change_broker(app_am_handle.clone()).await?;
        let cancel_token = CancellationToken::new();
        let (ticket, notif_rx) =
            InitStore::register_change_listener(&big_repo, &app_doc_id, vec![]).await?;
        let local_peer_id = big_repo.samod_repo().peer_id().to_string();

        let repo = Arc::new(Self {
            registry: Arc::clone(&registry),
            big_repo: Arc::clone(&big_repo),
            app_doc_id: app_doc_id.clone(),
            app_am_handle,
            store,
            local_actor_id,
            sql_pool,
            running_dispatches: default(),
            per_boot_done: default(),
            cancel_token: cancel_token.clone(),
            local_peer_id,
            _change_listener_tickets: vec![ticket],
            _change_broker_leases: vec![broker],
        });

        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let cancel_token = cancel_token.child_token();
            async move {
                repo.notifs_loop(notif_rx, cancel_token)
                    .await
                    .expect("error handling init repo notifs")
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

    pub fn init_id(plug_id: &str, plug_version: &semver::Version, init_key: &str) -> String {
        format!("{plug_id}@{plug_version}/{init_key}")
    }

    async fn notifs_loop(
        &self,
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<
            Vec<am_utils_rs::repo::BigRepoChangeNotification>,
        >,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        loop {
            let notifs = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                msg = notif_rx.recv() => match msg {
                    Some(notifs) => notifs,
                    None => break,
                }
            };
            let mut events = vec![];
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
                match &origin {
                    am_utils_rs::repo::BigRepoChangeOrigin::Local => continue,
                    am_utils_rs::repo::BigRepoChangeOrigin::Remote { peer_id, .. } => {
                        if peer_id.to_string() == self.local_peer_id {
                            continue;
                        }
                    }
                    am_utils_rs::repo::BigRepoChangeOrigin::Bootstrap => {}
                }
                self.events_for_patch(
                    &patch,
                    &heads,
                    &mut events,
                    Some(&origin),
                    Some(&self.local_peer_id),
                )
                .await?;
            }
            if events.is_empty() {
                continue;
            }
            if let Some(InitEvent::Changed { heads }) = events.last().cloned() {
                let Some((new_store, _)) = self
                    .big_repo
                    .hydrate_path_at_heads::<InitStore>(
                        &self.app_doc_id,
                        &heads.0,
                        automerge::ROOT,
                        vec![InitStore::prop().into()],
                    )
                    .await?
                else {
                    continue;
                };
                self.store
                    .mutate_sync(|store| {
                        store.per_install_done = new_store.per_install_done;
                    })
                    .await?;
                self.registry.notify(events.drain(..));
            }
        }
        Ok(())
    }

    pub async fn events_for_init(&self) -> Res<Vec<InitEvent>> {
        let heads = self.app_am_handle.with_document(|doc| doc.get_heads());
        Ok(vec![InitEvent::Changed {
            heads: ChangeHashSet(Arc::from(heads)),
        }])
    }

    pub async fn diff_events(
        &self,
        from: ChangeHashSet,
        to: Option<ChangeHashSet>,
    ) -> Res<Vec<InitEvent>> {
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
        let mut events = vec![];
        for patch in patches {
            self.events_for_patch(&patch, &heads.0, &mut events, None, None)
                .await?;
        }
        Ok(events)
    }

    async fn events_for_patch(
        &self,
        patch: &automerge::Patch,
        patch_heads: &Arc<[automerge::ChangeHash]>,
        out: &mut Vec<InitEvent>,
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
            &[InitStore::prop().into()],
            &patch.path,
        ) {
            return Ok(());
        }
        out.push(InitEvent::Changed {
            heads: ChangeHashSet(Arc::clone(patch_heads)),
        });
        Ok(())
    }

    pub async fn is_done(
        &self,
        run_mode: &daybook_types::manifest::InitRunMode,
        init_id: &str,
    ) -> Res<bool> {
        Ok(match run_mode {
            daybook_types::manifest::InitRunMode::PerInstall => {
                self.store
                    .query_sync(|store| {
                        store.per_install_done.contains_key(init_id)
                    })
                    .await
            }
            daybook_types::manifest::InitRunMode::PerNode => {
                let rec = sqlx::query_scalar::<_, String>(
                    "SELECT init_id FROM init_per_node WHERE init_id = ?1",
                )
                .bind(init_id)
                .fetch_optional(&self.sql_pool)
                .await?;
                rec.is_some()
            }
            daybook_types::manifest::InitRunMode::PerBoot => {
                self.per_boot_done.read().await.contains(init_id)
            }
        })
    }

    pub async fn mark_done(
        &self,
        run_mode: &daybook_types::manifest::InitRunMode,
        init_id: &str,
    ) -> Res<()> {
        match run_mode {
            daybook_types::manifest::InitRunMode::PerInstall => {
                let init_id = init_id.to_string();
                self.store
                    .mutate_sync(move |store| {
                        let deets = PerInstallDeets {
                            completed_at: jiff::Timestamp::now().to_string(),
                            completed_by_actor_id: self.local_actor_id.to_string(),
                        };
                        let versioned = match store.per_install_done.get(&init_id) {
                            Some(_) => Versioned::update(
                                self.local_actor_id.clone(),
                                ThroughJson(deets),
                            ),
                            None => {
                                Versioned::mint(self.local_actor_id.clone(), ThroughJson(deets))
                            }
                        };
                        store.per_install_done.insert(init_id, versioned);
                    })
                    .await?;
            }
            daybook_types::manifest::InitRunMode::PerNode => {
                sqlx::query(
                    r#"
                    INSERT INTO init_per_node(init_id, completed_at)
                    VALUES (?1, ?2)
                    ON CONFLICT(init_id) DO UPDATE SET completed_at = excluded.completed_at
                    "#,
                )
                .bind(init_id)
                .bind(jiff::Timestamp::now().to_string())
                .execute(&self.sql_pool)
                .await?;
            }
            daybook_types::manifest::InitRunMode::PerBoot => {
                self.per_boot_done.write().await.insert(init_id.to_string());
            }
        }
        Ok(())
    }

    pub async fn get_running_dispatch(&self, init_id: &str) -> Option<String> {
        let init_id = init_id.to_string();
        self.running_dispatches
            .read()
            .await
            .get(&init_id)
            .cloned()
    }

    pub async fn set_running_dispatch(&self, init_id: &str, dispatch_id: &str) -> Res<()> {
        self.running_dispatches
            .write()
            .await
            .insert(init_id.to_string(), dispatch_id.to_string());
        Ok(())
    }

    pub async fn clear_running_dispatch(&self, init_id: &str, dispatch_id: &str) -> Res<()> {
        let mut running = self.running_dispatches.write().await;
        if running
            .get(init_id)
            .map(|current| current == dispatch_id)
            .unwrap_or(false)
        {
            running.remove(init_id);
        }
        Ok(())
    }
}
