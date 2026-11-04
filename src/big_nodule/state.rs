//! The single owner of the reconciled nodule inventory, durable host actors, and applied drawer
//! grants.
//!
//! Desired state lives in the node's config drawer document and is projected through
//! [`DatumStoreHandle<NoduleConfig>`]. This task serializes management mutations and config
//! snapshot reconciliations, provisions/reopens each admitted nodule's durable actor under
//! `parent_path`, and applies or revokes service-owned local drawer grants within the nodules
//! actor's delegated authority.

use crate::store;
use big_cabinet::DatumStoreHandle;
use big_cabinet_core::authority::Actor;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::InspectCapability;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::ChangeHash;
use big_cabinet_core::doc::StoreError;
use big_cabinet_core::doc::StoreSnapshot;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::DrawersError;
use big_cabinet_core::drawer::protocol::DrawersProtocol;
use big_cabinet_core::drawer::protocol::GrantDrawer;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_nodule_core::config::AdmissionCache;
use big_nodule_core::config::NoduleConfig;
use big_nodule_core::config::NoduleResolution;
use big_nodule_core::inventory::InventoryError;
use big_nodule_core::inventory::Readiness;
use big_nodule_core::protocol::NoduleActor;
use big_nodule_core::protocol::NoduleGrantStatus;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::NoduleScope;
use big_nodule_core::protocol::NodulesError;
use big_plugs_core::Plug;
use big_plugs_core::PlugId;
use futures::StreamExt;
use indexmap::IndexMap;
use serde_json::Value;
use service_utils_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use service_utils_rs::StopToken;
use sqlite_rs::Database;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use utils_rs::expect_tags::ERROR_ACTOR;
use utils_rs::expect_tags::ERROR_CHANNEL;

type Res<T> = Result<T, NodulesError>;
type Reply<T> = oneshot::Sender<Res<T>>;

pub(crate) struct Stores {
    pub(crate) sql: Database,
    pub(crate) authority: Client<AuthorityProtocol>,
    pub(crate) drawers: Client<DrawersProtocol>,
    pub(crate) datum_store: DatumStoreHandle<NoduleConfig>,
    pub(crate) parent: Actor,
    pub(crate) parent_path: String,
}

pub(crate) enum Command {
    Admit {
        context: RequestContext,
        plug: Box<Plug>,
        drawers: Option<BTreeMap<DrawerId, DrawerAccess>>,
        reply: Reply<NoduleRecord>,
    },
    Configure {
        context: RequestContext,
        plug_id: PlugId,
        settings: IndexMap<String, Value>,
        reply: Reply<NoduleRecord>,
    },
    SetEnabled {
        context: RequestContext,
        plug_id: PlugId,
        enabled: bool,
        reply: Reply<NoduleRecord>,
    },
    SetDrawerGrants {
        context: RequestContext,
        plug_id: PlugId,
        drawers: BTreeMap<DrawerId, DrawerAccess>,
        reply: Reply<NoduleRecord>,
    },
    Uninstall {
        context: RequestContext,
        plug_id: PlugId,
        reply: Reply<()>,
    },
    Get {
        context: RequestContext,
        plug_id: PlugId,
        reply: Reply<NoduleRecord>,
    },
    List {
        context: RequestContext,
        scope: Option<NoduleScope>,
        reply: Reply<Vec<NoduleRecord>>,
    },
}

#[derive(Clone)]
pub(crate) struct Handle {
    commands: mpsc::Sender<Command>,
}

impl Handle {
    pub(crate) async fn ask<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> Res<T> {
        let (reply, answer) = oneshot::channel();
        self.commands.send(command(reply)).await.expect(ERROR_ACTOR);
        answer.await.expect(ERROR_CHANNEL)
    }
}

pub(crate) struct State {
    stores: Stores,
    persisted_actors: BTreeMap<PlugId, (String, Uuid7)>,
    actors: BTreeMap<PlugId, Actor>,
    applied_grants: BTreeMap<PlugId, BTreeMap<DrawerId, (DrawerAccess, CapId)>>,
    applied_heads: BTreeMap<PlugId, Vec<ChangeHash>>,
    records: BTreeMap<PlugId, NoduleRecord>,
    observed_heads: Vec<ChangeHash>,
    /// Memoized per-envelope admission, shared with the config-mutation closures.
    admission_cache: Arc<AdmissionCache>,
}

fn nodule_actor(actor: &Actor) -> NoduleActor {
    NoduleActor {
        actor_id: actor.actor_id,
        url: actor.url.clone(),
        public_key: actor.public_key.clone(),
    }
}

async fn answer<T>(mut reply: oneshot::Sender<T>, operation: impl Future<Output = T>) {
    let result = tokio::select! {
        biased;
        () = reply.closed() => return,
        result = operation => result,
    };
    drop(reply.send(result));
}

impl State {
    pub(crate) async fn recover(stores: Stores) -> Res<(Handle, StopToken)> {
        let mut persisted_actors = BTreeMap::new();
        for row in store::actors(&stores.sql).await? {
            persisted_actors.insert(row.plug_id, (row.nodule_id, row.actor_id));
        }
        let mut applied_grants: BTreeMap<PlugId, BTreeMap<DrawerId, (DrawerAccess, CapId)>> =
            BTreeMap::new();
        for row in store::applied_grants(&stores.sql).await? {
            applied_grants
                .entry(row.plug_id)
                .or_default()
                .insert(row.drawer_id, (row.access, row.capability_id));
        }

        let mut watch = stores
            .datum_store
            .watch()
            .await
            .map_err(crate::store_error)?;
        let initial = stores
            .datum_store
            .snapshot()
            .await
            .map_err(crate::store_error)?;

        let mut state = Self {
            stores,
            persisted_actors,
            actors: BTreeMap::new(),
            applied_grants,
            applied_heads: BTreeMap::new(),
            records: BTreeMap::new(),
            observed_heads: Vec::new(),
            admission_cache: Arc::default(),
        };
        let context = RequestContext::root();
        for (plug_id, (nodule_id, expected_actor_id)) in state.persisted_actors.clone() {
            let actor = state.stores.open_actor(&context, &nodule_id).await?;
            if actor.actor_id != expected_actor_id {
                return Err(NodulesError::Storage(format!(
                    "installed nodule `{plug_id}` was given actor {expected_actor_id} but its path now opens {}",
                    actor.actor_id
                )));
            }
            state.actors.insert(plug_id, actor);
        }
        state.reconcile(&context, initial, true).await?;

        let (commands, mut inbox) = mpsc::channel(64);
        let (stop, tasks) = StopToken::new();
        let cancellation = tasks.clone();
        // The loop's failures are programming or lifecycle errors, not per-nodule outcomes
        // (those live in the records `reconcile` builds), so the task unwraps its result: the
        // process panic handler then brings everything down instead of leaving a dead actor
        // that panics every later `Handle::ask` on a closed channel.
        let run = async move {
            loop {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => break,
                    command = inbox.recv() => match command {
                        Some(command) => state.run(command).await,
                        None => break,
                    },
                    event = watch.next() => match event {
                        // Revoke failures come back as a map of per-plug outcomes that
                        // `reconcile` already folded into the refused records, so only
                        // an `Err` (storage, actor or capability failure) is fatal.
                        Some(Ok(snapshot)) => {
                            drop(state.reconcile(&RequestContext::root(), snapshot, false).await?);
                        }
                        Some(Err(err)) => return Err(crate::store_error(err)),
                        // Shutdown stops this task before the datum store it watches, so a
                        // closed watch while we are still running is a lifecycle break.
                        None => {
                            return Err(NodulesError::Storage(
                                "nodule config watch closed while the state actor was running".into(),
                            ));
                        }
                    },
                }
            }
            Res::Ok(())
        };
        tasks
            .spawn(async move { run.await.expect("nodule state actor failed") })
            .expect("nodule state task starts before shutdown");
        Ok((Handle { commands }, stop))
    }

    async fn run(&mut self, command: Command) {
        match command {
            Command::Admit {
                context,
                plug,
                drawers,
                reply,
            } => answer(reply, self.admit(&context, *plug, drawers)).await,
            Command::Configure {
                context,
                plug_id,
                settings,
                reply,
            } => answer(reply, self.configure(&context, &plug_id, settings)).await,
            Command::SetEnabled {
                context,
                plug_id,
                enabled,
                reply,
            } => answer(reply, self.set_enabled(&context, &plug_id, enabled)).await,
            Command::SetDrawerGrants {
                context,
                plug_id,
                drawers,
                reply,
            } => answer(reply, self.set_drawer_grants(&context, &plug_id, drawers)).await,
            Command::Uninstall {
                context,
                plug_id,
                reply,
            } => answer(reply, self.uninstall(&context, &plug_id)).await,
            Command::Get {
                context,
                plug_id,
                reply,
            } => answer(reply, self.get(&context, &plug_id)).await,
            Command::List {
                context,
                scope,
                reply,
            } => answer(reply, self.list(&context, scope)).await,
        }
    }

    async fn mutate_config(
        &mut self,
        context: &RequestContext,
        edit: impl FnOnce(&mut NoduleConfig) -> Result<(), InventoryError> + Send + 'static,
    ) -> Res<()> {
        let domain_error: Arc<Mutex<Option<NodulesError>>> = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&domain_error);
        let result = self
            .stores
            .datum_store
            .mutate(context, None, move |config| {
                edit(config).map_err(|err| {
                    let message = err.to_string();
                    *captured.lock().expect("domain error lock") = Some(err.into());
                    StoreError::InvalidEdit(message)
                })
            })
            .await;
        if let Err(err) = result {
            if let Some(domain) = domain_error.lock().expect("domain error lock").take() {
                return Err(domain);
            }
            return Err(crate::store_error(err));
        }
        let snapshot = self
            .stores
            .datum_store
            .snapshot()
            .await
            .map_err(crate::store_error)?;
        drop(self.reconcile(context, snapshot, false).await?);
        Ok(())
    }

    async fn admit(
        &mut self,
        context: &RequestContext,
        plug: Plug,
        drawers: Option<BTreeMap<DrawerId, DrawerAccess>>,
    ) -> Res<NoduleRecord> {
        let incoming_nodule_id = plug
            .nodule()
            .and_then(|c| c.description.get("id"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        if let Some((saved_nodule_id, _)) = self.persisted_actors.get(&plug.id)
            && let Some(incoming_id) = &incoming_nodule_id
            && incoming_id != saved_nodule_id
        {
            return Err(InventoryError::AuthorityChanged {
                id: plug.id,
                existing: saved_nodule_id.clone(),
                incoming: incoming_id.clone(),
            }
            .into());
        }
        for (other_plug, (other_nodule_id, _)) in &self.persisted_actors {
            if other_plug != &plug.id
                && let Some(incoming_id) = &incoming_nodule_id
                && (owns_nsid(other_nodule_id, incoming_id)
                    || owns_nsid(incoming_id, other_nodule_id))
            {
                return Err(InventoryError::AuthorityConflict {
                    id: plug.id,
                    nodule_id: incoming_id.clone(),
                    existing_plug: Box::new(other_plug.clone()),
                    existing_nodule: other_nodule_id.clone(),
                }
                .into());
            }
        }
        let plug_id = plug.id.clone();
        let cache = Arc::clone(&self.admission_cache);
        self.mutate_config(context, move |config| {
            config.admit(&cache, &plug, drawers).map(|_| ())
        })
        .await?;
        self.records
            .get(&plug_id)
            .cloned()
            .ok_or(NodulesError::UnknownPlug(plug_id))
    }

    async fn configure(
        &mut self,
        context: &RequestContext,
        plug_id: &PlugId,
        settings: IndexMap<String, Value>,
    ) -> Res<NoduleRecord> {
        let target = plug_id.clone();
        self.mutate_config(context, move |config| {
            config.configure(&target, settings).map(|_| ())
        })
        .await?;
        self.records
            .get(plug_id)
            .cloned()
            .ok_or_else(|| NodulesError::UnknownPlug(plug_id.clone()))
    }

    async fn set_enabled(
        &mut self,
        context: &RequestContext,
        plug_id: &PlugId,
        enabled: bool,
    ) -> Res<NoduleRecord> {
        let target = plug_id.clone();
        self.mutate_config(context, move |config| {
            config.set_enabled(&target, enabled).map(|_| ())
        })
        .await?;
        self.records
            .get(plug_id)
            .cloned()
            .ok_or_else(|| NodulesError::UnknownPlug(plug_id.clone()))
    }

    async fn set_drawer_grants(
        &mut self,
        context: &RequestContext,
        plug_id: &PlugId,
        drawers: BTreeMap<DrawerId, DrawerAccess>,
    ) -> Res<NoduleRecord> {
        let target = plug_id.clone();
        self.mutate_config(context, move |config| {
            config.set_drawer_grants(&target, drawers).map(|_| ())
        })
        .await?;
        self.records
            .get(plug_id)
            .cloned()
            .ok_or_else(|| NodulesError::UnknownPlug(plug_id.clone()))
    }

    async fn uninstall(&mut self, context: &RequestContext, plug_id: &PlugId) -> Res<()> {
        let target = plug_id.clone();
        let domain_error: Arc<Mutex<Option<NodulesError>>> = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&domain_error);
        let result = self
            .stores
            .datum_store
            .mutate(context, None, move |config| {
                config.uninstall(&target).map_err(|err| {
                    let message = err.to_string();
                    *captured.lock().expect("domain error lock") = Some(err.into());
                    StoreError::InvalidEdit(message)
                })
            })
            .await;
        if let Err(err) = result {
            if let Some(domain) = domain_error.lock().expect("domain error lock").take() {
                return Err(domain);
            }
            return Err(crate::store_error(err));
        }
        let snapshot = self
            .stores
            .datum_store
            .snapshot()
            .await
            .map_err(crate::store_error)?;
        let revoke_errors = self.reconcile(context, snapshot, false).await?;
        if let Some(err) = revoke_errors.get(plug_id) {
            return Err(err.clone());
        }
        Ok(())
    }

    async fn refresh_if_needed(&mut self, context: &RequestContext) -> Res<()> {
        let snapshot = self
            .stores
            .datum_store
            .snapshot()
            .await
            .map_err(crate::store_error)?;
        let has_unapplied = self
            .records
            .values()
            .any(|record| record.applied_heads.as_ref() != Some(&record.observed_heads));
        // Config heads do not move when an applied grant's source capability is retracted
        // elsewhere (for instance when the nodules actor loses Admin on the drawer), so the
        // records would keep reporting `granted: true`. Check only those capabilities' liveness
        // and run the full reconcile, which re-applies or re-reports the grant, just when one
        // died.
        if snapshot.heads != self.observed_heads
            || has_unapplied
            || self.any_applied_grant_dead(context).await?
        {
            drop(self.reconcile(context, snapshot, false).await?);
        }
        Ok(())
    }

    async fn any_applied_grant_dead(&self, context: &RequestContext) -> Res<bool> {
        for (_, cap_id) in self.applied_grants.values().flat_map(BTreeMap::values) {
            if !self.stores.is_cap_live(context, *cap_id).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn get(&mut self, context: &RequestContext, plug_id: &PlugId) -> Res<NoduleRecord> {
        self.refresh_if_needed(context).await?;
        self.records
            .get(plug_id)
            .cloned()
            .ok_or_else(|| NodulesError::UnknownPlug(plug_id.clone()))
    }

    async fn list(
        &mut self,
        context: &RequestContext,
        scope: Option<NoduleScope>,
    ) -> Res<Vec<NoduleRecord>> {
        self.refresh_if_needed(context).await?;
        Ok(self
            .records
            .iter()
            .filter(|(plug_id, _)| {
                scope
                    .as_ref()
                    .is_none_or(|scope| scope.plugs.contains(plug_id))
            })
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn reconcile(
        &mut self,
        context: &RequestContext,
        snapshot: StoreSnapshot<NoduleConfig>,
        on_boot: bool,
    ) -> Res<BTreeMap<PlugId, NodulesError>> {
        let mut resolved = snapshot.value.resolve(&self.admission_cache);
        for (plug_id, (saved_nodule_id, _)) in &self.persisted_actors {
            if let Some(NoduleResolution::Admitted { installed, .. }) = resolved.get(plug_id)
                && &installed.nodule.id != saved_nodule_id
            {
                let reason = InventoryError::AuthorityChanged {
                    id: plug_id.clone(),
                    existing: saved_nodule_id.clone(),
                    incoming: installed.nodule.id.clone(),
                }
                .to_string();
                let Some(NoduleResolution::Admitted { installed, .. }) = resolved.remove(plug_id)
                else {
                    unreachable!();
                };
                resolved.insert(
                    plug_id.clone(),
                    NoduleResolution::Refused {
                        plug_id: plug_id.clone(),
                        version: Some(installed.version),
                        revision: Some(installed.revision),
                        nodule_id: Some(installed.nodule.id),
                        enabled: installed.enabled,
                        config: installed.config,
                        reason,
                    },
                );
            }
        }

        let mut revoke_errors: BTreeMap<PlugId, NodulesError> = BTreeMap::new();
        let active_plugs: Vec<PlugId> = self.applied_grants.keys().cloned().collect();
        for plug_id in active_plugs {
            if !matches!(
                resolved.get(&plug_id),
                Some(NoduleResolution::Admitted { .. })
            ) {
                if let Err(err) = self
                    .revoke_grants_for_plug(context, &plug_id, &BTreeMap::new())
                    .await
                {
                    revoke_errors.insert(plug_id.clone(), err);
                }
                if !resolved.contains_key(&plug_id) {
                    self.applied_heads.remove(&plug_id);
                }
            }
        }

        let mut new_records = BTreeMap::new();
        for (plug_id, resolution) in resolved {
            match resolution {
                NoduleResolution::Refused {
                    plug_id,
                    version,
                    revision,
                    nodule_id,
                    enabled,
                    config,
                    reason,
                } => {
                    let error = match revoke_errors.get(&plug_id) {
                        Some(revoke_err) => format!("{reason}; {revoke_err}"),
                        None => reason,
                    };
                    new_records.insert(
                        plug_id.clone(),
                        NoduleRecord {
                            plug_id: plug_id.clone(),
                            version,
                            revision,
                            nodule_id,
                            actor: self.actors.get(&plug_id).map(nodule_actor),
                            enabled,
                            readiness: Readiness::Refused,
                            config,
                            artifacts: BTreeMap::new(),
                            drawers: BTreeMap::new(),
                            observed_heads: snapshot.heads.clone(),
                            applied_heads: self.applied_heads.get(&plug_id).cloned(),
                            error: Some(error),
                        },
                    );
                }
                NoduleResolution::Admitted {
                    installed,
                    desired_grants,
                } => {
                    let record = self
                        .reconcile_admitted(
                            context,
                            &snapshot.heads,
                            on_boot,
                            installed,
                            desired_grants,
                        )
                        .await?;
                    new_records.insert(plug_id, record);
                }
            }
        }

        self.observed_heads = snapshot.heads;
        self.records = new_records;
        Ok(revoke_errors)
    }

    async fn ensure_nodule_actor(
        &mut self,
        context: &RequestContext,
        plug_id: &PlugId,
        nodule_id: &str,
    ) -> Res<Actor> {
        if let Some(actor) = self.actors.get(plug_id) {
            return Ok(actor.clone());
        }
        let opened = self.stores.open_actor(context, nodule_id).await?;
        if let Some((saved_nodule_id, saved_actor_id)) = self.persisted_actors.get(plug_id) {
            if saved_nodule_id != nodule_id || opened.actor_id != *saved_actor_id {
                return Err(NodulesError::Storage(format!(
                    "installed nodule `{plug_id}` was given actor {saved_actor_id} but its path now opens {}",
                    opened.actor_id
                )));
            }
        } else {
            store::upsert_actor(&self.stores.sql, plug_id, nodule_id, opened.actor_id).await?;
            self.persisted_actors
                .insert(plug_id.clone(), (nodule_id.to_owned(), opened.actor_id));
        }
        self.actors.insert(plug_id.clone(), opened.clone());
        Ok(opened)
    }

    async fn reconcile_admitted(
        &mut self,
        context: &RequestContext,
        heads: &[ChangeHash],
        on_boot: bool,
        installed: big_nodule_core::inventory::InstalledNodule,
        desired_grants: BTreeMap<DrawerId, DrawerAccess>,
    ) -> Res<NoduleRecord> {
        let plug_id = installed.plug_id.clone();
        let actor = self
            .ensure_nodule_actor(context, &plug_id, &installed.nodule.id)
            .await?;

        let mut apply_errors = Vec::new();
        if let Err(err) = self
            .revoke_grants_for_plug(context, &plug_id, &desired_grants)
            .await
        {
            apply_errors.push(err.to_string());
        }

        let mut drawer_statuses = BTreeMap::new();
        for (drawer_id, desired_access) in &desired_grants {
            let existing = self
                .applied_grants
                .get(&plug_id)
                .and_then(|grants| grants.get(drawer_id))
                .copied();
            if let Some((applied_access, cap_id)) = existing
                && applied_access == *desired_access
            {
                let live = self.stores.is_cap_live(context, cap_id).await?;
                if live {
                    if on_boot {
                        if self
                            .stores
                            .grant_with_cap(
                                context,
                                cap_id,
                                drawer_id,
                                actor.actor_id,
                                *desired_access,
                            )
                            .await
                            .is_ok()
                        {
                            drawer_statuses.insert(
                                drawer_id.clone(),
                                NoduleGrantStatus {
                                    desired: *desired_access,
                                    granted: true,
                                    error: None,
                                },
                            );
                            continue;
                        }
                        drop(self.stores.retract_grant_cap(context, cap_id).await);
                    } else {
                        drawer_statuses.insert(
                            drawer_id.clone(),
                            NoduleGrantStatus {
                                desired: *desired_access,
                                granted: true,
                                error: None,
                            },
                        );
                        continue;
                    }
                }
                store::delete_applied_grant(&self.stores.sql, &plug_id, drawer_id).await?;
                if let Some(grants) = self.applied_grants.get_mut(&plug_id) {
                    grants.remove(drawer_id);
                }
            }

            match self
                .stores
                .apply_grant(
                    &self.stores.sql,
                    context,
                    &plug_id,
                    drawer_id,
                    actor.actor_id,
                    *desired_access,
                )
                .await
            {
                Ok(cap_id) => {
                    self.applied_grants
                        .entry(plug_id.clone())
                        .or_default()
                        .insert(drawer_id.clone(), (*desired_access, cap_id));
                    drawer_statuses.insert(
                        drawer_id.clone(),
                        NoduleGrantStatus {
                            desired: *desired_access,
                            granted: true,
                            error: None,
                        },
                    );
                }
                Err(NodulesError::Storage(err)) => return Err(NodulesError::Storage(err)),
                Err(err) => {
                    let msg = err.to_string();
                    apply_errors.push(format!("drawer `{drawer_id}`: {msg}"));
                    drawer_statuses.insert(
                        drawer_id.clone(),
                        NoduleGrantStatus {
                            desired: *desired_access,
                            granted: false,
                            error: Some(msg),
                        },
                    );
                }
            }
        }

        let error = if apply_errors.is_empty() {
            self.applied_heads.insert(plug_id.clone(), heads.to_vec());
            None
        } else {
            Some(apply_errors.join("; "))
        };

        Ok(NoduleRecord {
            plug_id: installed.plug_id.clone(),
            version: Some(installed.version.clone()),
            revision: Some(installed.revision),
            nodule_id: Some(installed.nodule.id.clone()),
            actor: Some(nodule_actor(&actor)),
            enabled: installed.enabled,
            readiness: installed.readiness(),
            config: installed.config.clone(),
            artifacts: installed
                .artifacts
                .iter()
                .map(|(key, (_, state))| (key.clone(), *state))
                .collect(),
            drawers: drawer_statuses,
            observed_heads: heads.to_vec(),
            applied_heads: self.applied_heads.get(&plug_id).cloned(),
            error,
        })
    }

    async fn revoke_grants_for_plug(
        &mut self,
        context: &RequestContext,
        plug_id: &PlugId,
        keep: &BTreeMap<DrawerId, DrawerAccess>,
    ) -> Res<()> {
        let Some(existing) = self.applied_grants.get(plug_id).cloned() else {
            return Ok(());
        };
        let mut first_err: Option<NodulesError> = None;
        for (drawer_id, (applied_access, cap_id)) in existing {
            if keep.get(&drawer_id) == Some(&applied_access) {
                continue;
            }
            match self.stores.retract_grant_cap(context, cap_id).await {
                Ok(()) => {
                    store::delete_applied_grant(&self.stores.sql, plug_id, &drawer_id).await?;
                    if let Some(grants) = self.applied_grants.get_mut(plug_id) {
                        grants.remove(&drawer_id);
                    }
                }
                Err(err) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
        }
        if self
            .applied_grants
            .get(plug_id)
            .is_some_and(BTreeMap::is_empty)
        {
            self.applied_grants.remove(plug_id);
        }
        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

fn owns_nsid(authority: &str, nsid: &str) -> bool {
    nsid == authority
        || nsid
            .strip_prefix(authority)
            .is_some_and(|rest| rest.starts_with('.'))
}

impl Stores {
    async fn open_actor(&self, context: &RequestContext, nodule_id: &str) -> Res<Actor> {
        self.authority
            .call(
                context.child(),
                OpenActor {
                    parent: self.parent.capability,
                    path: format!("{}/{nodule_id}", self.parent_path),
                    rights: Rights::USE,
                },
            )
            .await
            .map_err(crate::authority_error)
    }

    async fn is_cap_live(&self, context: &RequestContext, capability: CapId) -> Res<bool> {
        match self
            .authority
            .call(context.child(), InspectCapability { capability })
            .await
        {
            Ok(_) => Ok(true),
            Err(ClientError::Operation(AuthorityServiceError::InvalidCapability)) => Ok(false),
            Err(error) => Err(crate::authority_error(error)),
        }
    }

    async fn retract_grant_cap(&self, context: &RequestContext, capability: CapId) -> Res<()> {
        match self
            .authority
            .call(
                context.child(),
                RetractCapability {
                    via: capability,
                    capability,
                },
            )
            .await
        {
            Ok(()) | Err(ClientError::Operation(AuthorityServiceError::InvalidCapability)) => {
                Ok(())
            }
            Err(error) => Err(crate::authority_error(error)),
        }
    }

    async fn grant_with_cap(
        &self,
        context: &RequestContext,
        capability: CapId,
        drawer_id: &DrawerId,
        subject_id: Uuid7,
        access: DrawerAccess,
    ) -> Res<()> {
        self.drawers
            .call(
                context.child(),
                GrantDrawer {
                    capability,
                    drawer_id: drawer_id.clone(),
                    subject_id,
                    access,
                },
            )
            .await
            .map_err(drawers_error)
    }

    async fn apply_grant(
        &self,
        sql: &Database,
        context: &RequestContext,
        plug_id: &PlugId,
        drawer_id: &DrawerId,
        subject_id: Uuid7,
        access: DrawerAccess,
    ) -> Res<CapId> {
        let cap = self
            .drawers
            .call(
                context.child(),
                MintDrawerCapability {
                    authorization: Authorization::identity(self.parent.capability),
                    scope: DrawerScope {
                        drawers: BTreeMap::from([(drawer_id.clone(), DrawerAccess::Admin)]),
                        create: false,
                    },
                    rights: Rights::USE | Rights::DELEGATE,
                },
            )
            .await
            .map_err(drawers_error)?;
        let cap_id = cap.capability_id;
        store::upsert_applied_grant(sql, plug_id, drawer_id, access, cap_id).await?;
        if let Err(err) = self
            .grant_with_cap(context, cap_id, drawer_id, subject_id, access)
            .await
        {
            drop(self.retract_grant_cap(context, cap_id).await);
            drop(store::delete_applied_grant(sql, plug_id, drawer_id).await);
            return Err(err);
        }
        Ok(cap_id)
    }
}

fn drawers_error(error: ClientError<DrawersError>) -> NodulesError {
    match error {
        ClientError::Operation(DrawersError::Context(error)) => error.into(),
        ClientError::Operation(DrawersError::Authority(error)) => NodulesError::Authority(error),
        ClientError::Operation(DrawersError::AuthorityUnavailable(msg)) => {
            NodulesError::AuthorityUnavailable(msg)
        }
        ClientError::Operation(other) => NodulesError::Rejected(other.to_string()),
        ClientError::Transport { message, .. } => NodulesError::Storage(message),
    }
}
