//! FIXME: make this more memory efficent
//! - Don't hold the Store in memory but hydrate as needed
//! - Use LRU cache for Doc cache

use crate::interlude::*;

use automerge::transaction::Transactable;
use automerge::ReadDoc;
use daybook_types::doc::{AddDocArgs, ChangeHashSet, Doc, DocId, DocPatch};
use tokio_util::sync::CancellationToken;
use utils_rs::am::changes::ChangeNotification;
// Automerge types for hydrate/reconcile boundaries
// We use the conversion functions from daybook_types::automerge module
// The automerge::Doc type is accessed through conversions
use crate::repos::Repo;
use std::str::FromStr;

#[derive(Default, Debug, Reconcile, Hydrate)]
pub struct DrawerStore {
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    pub map: HashMap<DocId, DocEntry>,
}

#[derive(Debug, Clone, PartialEq, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct PropBlame {
    heads: ChangeHashSet,
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct DocEntry {
    pub branches: HashMap<String, ChangeHashSet>,
    // techincally, we can recover this information
    // from the doc but we mantain it here for quick access
    // FIXME: consider lazily recreating this on load instead
    // of mantaining it in the automerge state
    pub prop_blames: HashMap<String, PropBlame>,
    // Mapping from ActorId string to UserMeta
    pub users: HashMap<String, crate::config::UserMeta>,
    // WARN: field ordering is imporant here, we want reconciliation
    // to create changes on the map before the atomic map so that changes
    // to the atmoic version increment will be always observed after the
    // other fields
    pub version: Uuid,
    pub previous_version_heads: Option<ChangeHashSet>,
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct DocNBranches {
    pub doc_id: DocId,
    pub branches: HashMap<String, ChangeHashSet>,
}

impl DocNBranches {
    pub fn main_branch_path(&self) -> Option<daybook_types::doc::BranchPath> {
        if self.branches.contains_key("main") {
            Some(daybook_types::doc::BranchPath::from("main"))
        } else {
            self.branches.keys().next().map(|k| daybook_types::doc::BranchPath::from(k.as_str()))
        }
    }
}

#[async_trait]
impl crate::stores::Store for DrawerStore {
    // type FlushArgs = (AmCtx, DocumentId);
    // const PATH: &[&str] = &["docs"];
    const PROP: &str = "docs";
}

pub struct DrawerRepo {
    // drawer_doc_id: DocumentId,
    pub acx: AmCtx,
    store: crate::stores::StoreHandle<DrawerStore>,
    // in-memory cache of document handles
    handles: Arc<DHashMap<DocId, samod::DocHandle>>,
    // we only cache a single version of the doc
    // in memory
    cache: Arc<DHashMap<DocId, (Arc<Doc>, ChangeHashSet)>>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    drawer_doc_id: DocumentId,
    pub local_actor_id: automerge::ActorId,
    cancel_token: CancellationToken,
    _change_listener_tickets: Vec<utils_rs::am::changes::ChangeListenerRegistration>,
    drawer_am_handle: samod::DocHandle,
    current_heads: std::sync::RwLock<ChangeHashSet>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct HeadDiff {
    pub from: Option<ChangeHashSet>,
    pub to: Option<ChangeHashSet>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct PropBlameDiff {
    pub from: Option<PropBlame>,
    pub to: Option<PropBlame>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct UserMetaDiff {
    pub from: Option<crate::config::UserMeta>,
    pub to: Option<crate::config::UserMeta>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct DocEntryDiff {
    pub branches: HashMap<daybook_types::doc::BranchPath, HeadDiff>,
    pub prop_blames: HashMap<String, PropBlameDiff>,
    pub users: HashMap<String, UserMetaDiff>,
}

impl DocEntry {
    pub fn diff(&self, old: &Self) -> DocEntryDiff {
        let mut branches = HashMap::new();
        for (key, old_hash) in &old.branches {
            let branch_path = daybook_types::doc::BranchPath::from(key.as_str());
            match self.branches.get(key) {
                Some(new_hash) if *new_hash == *old_hash => {}
                Some(new_hash) => {
                    branches.insert(
                        branch_path,
                        HeadDiff {
                            from: Some(old_hash.clone()),
                            to: Some(new_hash.clone()),
                        },
                    );
                }
                None => {
                    branches.insert(
                        branch_path,
                        HeadDiff {
                            from: Some(old_hash.clone()),
                            to: None,
                        },
                    );
                }
            }
        }
        for (key, new_hash) in &self.branches {
            let branch_path = daybook_types::doc::BranchPath::from(key.as_str());
            if !old.branches.contains_key(key) {
                branches.insert(
                    branch_path,
                    HeadDiff {
                        from: None,
                        to: Some(new_hash.clone()),
                    },
                );
            }
        }

        let mut prop_blames = HashMap::new();
        for (key, old_blame) in &old.prop_blames {
            match self.prop_blames.get(key) {
                Some(new_blame) if new_blame == old_blame => {}
                Some(new_blame) => {
                    prop_blames.insert(
                        key.clone(),
                        PropBlameDiff {
                            from: Some(old_blame.clone()),
                            to: Some(new_blame.clone()),
                        },
                    );
                }
                None => {
                    prop_blames.insert(
                        key.clone(),
                        PropBlameDiff {
                            from: Some(old_blame.clone()),
                            to: None,
                        },
                    );
                }
            }
        }
        for (key, new_blame) in &self.prop_blames {
            if !prop_blames.contains_key(key) {
                prop_blames.insert(
                    key.clone(),
                    PropBlameDiff {
                        from: None,
                        to: Some(new_blame.clone()),
                    },
                );
            }
        }

        let mut users = HashMap::new();
        for (key, old_user) in &old.users {
            match self.users.get(key) {
                Some(new_user) if new_user == old_user => {}
                Some(new_user) => {
                    users.insert(
                        key.clone(),
                        UserMetaDiff {
                            from: Some(old_user.clone()),
                            to: Some(new_user.clone()),
                        },
                    );
                }
                None => {
                    users.insert(
                        key.clone(),
                        UserMetaDiff {
                            from: Some(old_user.clone()),
                            to: None,
                        },
                    );
                }
            }
        }
        for (key, new_user) in &self.users {
            if !users.contains_key(key) {
                users.insert(
                    key.clone(),
                    UserMetaDiff {
                        from: None,
                        to: Some(new_user.clone()),
                    },
                );
            }
        }

        DocEntryDiff {
            branches,
            prop_blames,
            users,
        }
    }
}

// Minimal event enum so Kotlin can refresh via ffiList on changes
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DrawerEvent {
    ListChanged {
        drawer_heads: ChangeHashSet,
    },
    DocAdded {
        id: DocId,
        entry: DocEntry,
        drawer_heads: ChangeHashSet,
    },
    DocUpdated {
        id: DocId,
        entry: DocEntry,
        diff: DocEntryDiff,
        drawer_heads: ChangeHashSet,
    },
    DocDeleted {
        id: DocId,
        entry: DocEntry, // old_heads: ChangeHashSet,
        drawer_heads: ChangeHashSet,
    },
}

#[derive(Debug)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct UpdateDocArgs {
    branch_path: daybook_types::doc::BranchPath,
    heads: Option<ChangeHashSet>,
    patch: DocPatch,
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum UpdateDocErr {
    /// patch for unrecognized document: {id}
    DocNotFound { id: DocId },
    /// headless patch for unrecognized branch: {name}
    BranchNotFound { name: String },
    /// patch has an invalid key: {inner}
    InvalidKey {
        #[from]
        inner: daybook_types::doc::DocPropTagParseError,
    },
    /// unexpected error: {inner}
    Other {
        #[from]
        inner: eyre::Report,
    },
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
/// error applying some patches at given indices: {map:?}
pub struct UpdateDocBatchErr {
    map: HashMap<u64, UpdateDocErr>,
}

pub enum DrawerUpdate {}

impl DrawerRepo {
    fn current_heads(&self) -> ChangeHashSet {
        tokio::task::block_in_place(|| self.current_heads.read().unwrap().clone())
    }

    fn update_current_heads(&self, heads: ChangeHashSet) {
        tokio::task::block_in_place(|| *self.current_heads.write().unwrap() = heads);
    }

    pub async fn load(
        acx: AmCtx,
        drawer_doc_id: DocumentId,
        local_actor_id: automerge::ActorId,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let registry = crate::repos::ListenersRegistry::new();

        let store_val = DrawerStore::load(&acx, &drawer_doc_id).await?;
        let drawer_am_handle = acx
            .find_doc(&drawer_doc_id)
            .await?
            .expect("doc should have been loaded");

        let initial_heads =
            drawer_am_handle.with_document(|doc| ChangeHashSet(doc.get_heads().into()));

        let store = crate::stores::StoreHandle::new(
            store_val,
            acx.clone(),
            drawer_doc_id.clone(),
            local_actor_id.clone(),
        );
        let (broker, broker_stop) = {
            acx.change_manager()
                .add_doc(drawer_am_handle.clone())
                .await?
        };

        let (notif_tx, notif_rx) =
            tokio::sync::mpsc::unbounded_channel::<Vec<ChangeNotification>>();
        let ticket = DrawerStore::register_change_listener(&acx, &broker, vec!["map".into()], {
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
            drawer_doc_id,
            drawer_am_handle,
            store,
            local_actor_id,
            registry: Arc::clone(&registry),
            handles: default(),
            cache: default(),
            cancel_token: main_cancel_token.child_token(),
            _change_listener_tickets: vec![ticket],
            current_heads: std::sync::RwLock::new(initial_heads),
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
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<ChangeNotification>>,
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
            let mut last_heads = None;

            for notif in notifs {
                last_heads = Some(notif.heads.clone());

                // 1. Extract ActorId from the patch using the new utils_rs::am helper.
                if let Some(actor_id) = utils_rs::am::get_actor_id_from_patch(&notif.patch) {
                    // 2. Skip if it matches self.local_actor_id.
                    if actor_id == self.local_actor_id {
                        debug!("process_notifs: skipping local change");
                        continue;
                    }
                }

                // 3. Call events_for_patch (pure - no side effects on self.store).
                self.events_for_patch(&notif.patch, &notif.heads, &mut events)
                    .await?;
            }

            for event in events.iter_mut() {
                match event {
                    DrawerEvent::DocAdded { id, entry, .. } => {
                        let entry_copy = entry.clone();
                        self.store
                            .mutate_sync(|store| {
                                store.map.insert(id.clone(), entry_copy);
                            })
                            .await?;
                    }
                    DrawerEvent::DocUpdated { id, entry, .. } => {
                        let entry_copy = entry.clone();
                        self.store
                            .mutate_sync(|store| store.map.insert(id.clone(), entry_copy))
                            .await?;
                    }
                    DrawerEvent::DocDeleted { id, .. } => {
                        self.store.mutate_sync(|store| store.map.remove(id)).await?;
                    }
                    DrawerEvent::ListChanged { .. } => {}
                }
            }

            if !events.is_empty() {
                let drawer_heads = ChangeHashSet(last_heads.expect("notifs not empty"));

                self.update_current_heads(drawer_heads.clone());

                self.registry.notify(
                    events
                        .drain(..)
                        .chain(std::iter::once(DrawerEvent::ListChanged { drawer_heads })),
                );
            }
        }
        Ok(())
    }

    async fn events_for_patch(
        &self,
        patch: &automerge::Patch,
        patch_heads: &Arc<[automerge::ChangeHash]>,
        out: &mut Vec<DrawerEvent>,
    ) -> Res<()> {
        if !utils_rs::am::changes::path_prefix_matches(
            &[DrawerStore::PROP.into(), "map".into()],
            &patch.path,
        ) {
            return Ok(());
        }

        let drawer_heads = ChangeHashSet(patch_heads.clone());

        match &patch.action {
            automerge::PatchAction::PutMap {
                value: (val, _obj_id),
                key,
                ..
            } if patch.path.len() == 3 && key == "version" => {
                let Some((_obj, automerge::Prop::Map(doc_id_str))) = patch.path.get(2) else {
                    return Ok(());
                };
                let doc_id = DocId::from(doc_id_str.clone());

                let version_bytes = match val {
                    automerge::Value::Scalar(s) => match &**s {
                        automerge::ScalarValue::Bytes(b) => b,
                        _ => return Ok(()),
                    },
                    _ => return Ok(()),
                };
                let version = Uuid::from_slice(version_bytes)?;

                // Hydrate the current entry
                let (new_entry, _) = self
                    .acx
                    .hydrate_path_at_heads::<DocEntry>(
                        &self.drawer_doc_id,
                        &patch_heads,
                        automerge::ROOT,
                        vec![
                            DrawerStore::PROP.into(),
                            "map".into(),
                            autosurgeon::Prop::Key(doc_id.to_string().into()),
                        ],
                    )
                    .await?
                    .expect(ERROR_INVALID_PATCH);

                if version.is_nil() {
                    out.push(DrawerEvent::DocAdded {
                        id: doc_id,
                        entry: new_entry,
                        drawer_heads,
                    });
                } else {
                    let diff = if let Some(prev_heads) = &new_entry.previous_version_heads {
                        let (old_entry, _) = self
                            .acx
                            .hydrate_path_at_heads::<DocEntry>(
                                &self.drawer_doc_id,
                                &prev_heads.0,
                                automerge::ROOT,
                                vec![
                                    DrawerStore::PROP.into(),
                                    "map".into(),
                                    autosurgeon::Prop::Key(doc_id.to_string().into()),
                                ],
                            )
                            .await?
                            .expect(ERROR_INVALID_PATCH);

                        new_entry.diff(&old_entry)
                    } else {
                        DocEntryDiff {
                            branches: new_entry
                                .branches
                                .iter()
                                .map(|(key, val)| {
                                    (
                                        daybook_types::doc::BranchPath::from(key.as_str()),
                                        HeadDiff {
                                            from: None,
                                            to: Some(val.clone()),
                                        },
                                    )
                                })
                                .collect(),
                            prop_blames: new_entry
                                .prop_blames
                                .iter()
                                .map(|(key, val)| {
                                    (
                                        key.clone(),
                                        PropBlameDiff {
                                            from: None,
                                            to: Some(val.clone()),
                                        },
                                    )
                                })
                                .collect(),
                            users: new_entry
                                .users
                                .iter()
                                .map(|(key, val)| {
                                    (
                                        key.clone(),
                                        UserMetaDiff {
                                            from: None,
                                            to: Some(val.clone()),
                                        },
                                    )
                                })
                                .collect(),
                        }
                    };

                    out.push(DrawerEvent::DocUpdated {
                        id: doc_id,
                        entry: new_entry,
                        diff,
                        drawer_heads,
                    });
                }
            }
            automerge::PatchAction::DeleteMap { key, .. } if patch.path.len() == 2 => {
                let doc_id = DocId::from(key.clone());
                out.push(DrawerEvent::DocDeleted {
                    id: doc_id,
                    entry: DocEntry {
                        branches: default(),
                        prop_blames: default(),
                        users: default(),
                        version: Uuid::nil(),
                        previous_version_heads: None,
                    },
                    drawer_heads,
                });
            }
            _ => {}
        }
        Ok(())
    }

    // NOTE: old contains/insert/remove removed. Use add/get/update/del instead.

    pub async fn list(&self) -> Vec<DocNBranches> {
        self.store
            .query_sync(|store| {
                store
                    .map
                    .iter()
                    .map(|(doc_id, entry)| DocNBranches {
                        doc_id: doc_id.clone(),
                        branches: entry.branches.clone(),
                    })
                    .collect()
            })
            .await
    }

    pub async fn diff_events(
        &self,
        from: ChangeHashSet,
        to: Option<ChangeHashSet>,
    ) -> Res<Vec<DrawerEvent>> {
        let (patches, heads) = self.drawer_am_handle.with_document(|am_doc| {
            let heads = if let Some(ref to_set) = to {
                to_set.clone()
            } else {
                ChangeHashSet(am_doc.get_heads().into())
            };
            let patches = am_doc
                .diff_obj(&automerge::ROOT, &from, &heads, true)
                .wrap_err("diff_obj failed")?;
            eyre::Ok((patches, heads))
        })?;
        let heads = heads.0;
        let mut events = vec![];
        for patch in patches {
            self.events_for_patch(&patch, &heads, &mut events).await?;
        }
        Ok(events)
    }

    pub async fn add(&self, args: AddDocArgs) -> Res<DocId> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }

        let mutation_actor_id = if let Some(path) = &args.user_path {
            daybook_types::doc::user_path::to_actor_id(path)
        } else {
            self.local_actor_id.clone()
        };

        // Use AutoCommit for reconciliation
        let mut doc_am = automerge::Automerge::new();
        doc_am.set_actor(mutation_actor_id);
        let handle = self.acx.add_doc(doc_am).await?;

        let new_doc = Doc {
            id: handle.document_id().to_string(),
            created_at: Timestamp::now(),
            updated_at: Timestamp::now(),
            props: args.props,
        };

        let prop_keys = new_doc.props.keys().cloned().collect::<Vec<_>>();

        let (new_doc, heads) = handle.with_document(move |doc_am| {
            doc_am.set_actor(self.local_actor_id.clone());
            let doc = doc_am
                .transact(move |tx| {
                    use automerge::transaction::Transactable;
                    tx.put(automerge::ROOT, "$schema", "daybook.doc")?;
                    let new_doc = ThroughJson(new_doc);
                    autosurgeon::reconcile(tx, &new_doc)
                        .map_err(|err| ferr!(err.to_string()))
                        .wrap_err("error reconciling new doc")?;
                    eyre::Ok(new_doc.0)
                })
                .map(|val| val.result)
                .map_err(|err| err.error)?;
            eyre::Ok((doc, doc_am.get_heads()))
        })?;
        let new_doc = Arc::new(new_doc);
        let heads = ChangeHashSet(heads.into());
        let mut users = HashMap::new();
        if let Some(user_path) = args.user_path {
            users.insert(
                self.local_actor_id.to_string(),
                crate::config::UserMeta {
                    user_path,
                    seen_at: Timestamp::now(),
                }
                .into(),
            );
        }
        let entry = DocEntry {
            prop_blames: prop_keys
                .into_iter()
                .map(|key| {
                    (
                        key.to_string(),
                        PropBlame {
                            heads: heads.clone(),
                        },
                    )
                })
                .collect(),
            branches: [(args.branch_path.to_string_lossy().to_string(), heads.clone())].into(),
            users,
            version: Uuid::nil(),
            previous_version_heads: None,
        };

        // store id in drawer AM
        let (_, drawer_heads) = self
            .store
            .mutate_sync(|store| {
                let old = store.map.insert(new_doc.id.clone(), entry.clone());
                assert!(old.is_none(), "fishy");
            })
            .await?;
        let drawer_heads = ChangeHashSet(drawer_heads.into_iter().collect());
        self.update_current_heads(drawer_heads.clone());

        // cache the handle under the doc's Uuid id
        let out_id = new_doc.id.clone();
        self.cache
            .insert(new_doc.id.clone(), (new_doc, heads.clone()));
        self.handles.insert(out_id.clone(), handle);
        self.registry.notify([
            DrawerEvent::DocAdded {
                id: out_id.clone(),
                entry,
                drawer_heads: drawer_heads.clone(),
            },
            DrawerEvent::ListChanged { drawer_heads },
        ]);
        Ok(out_id)
    }

    async fn get_handle(&self, id: &DocId) -> Res<Option<samod::DocHandle>> {
        match self.handles.get(id) {
            Some(handle) => Ok(Some(handle.clone())),
            None => {
                // Not in cache: check if the drawer actually lists this id
                if !(self
                    .store
                    .query_sync(|store| store.map.contains_key(id))
                    .await)
                {
                    return Ok(None);
                }
                let doc_id = DocumentId::from_str(id).wrap_err("invalid id")?;
                let Some(handle) = self.acx.find_doc(&doc_id).await? else {
                    return Ok(None);
                };

                self.handles.insert(id.clone(), handle.clone());

                Ok(Some(handle))
            }
        }
    }

    pub async fn get(&self, id: &DocId, branch_path: &daybook_types::doc::BranchPath) -> Res<Option<Arc<Doc>>> {
        self.get_with_heads(id, branch_path)
            .await
            .map(|opt| opt.map(|(doc, _)| doc))
    }

    pub async fn get_doc_branches(&self, doc_id: &DocId) -> Option<DocNBranches> {
        self.store
            .query_sync(|store| {
                store.map.get(doc_id).map(|entry| DocNBranches {
                    doc_id: doc_id.clone(),
                    branches: entry.branches.clone(),
                })
            })
            .await
    }

    pub async fn get_if_latest(
        &self,
        id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        heads: &ChangeHashSet,
    ) -> Res<Option<Arc<Doc>>> {
        let is_latest = self
            .store
            .query_sync(|store| {
                store
                    .map
                    .get(id)
                    .and_then(|entry| entry.branches.get(&branch_path.to_string_lossy().to_string()))
                    .map(|latest_heads| latest_heads == heads)
                    .unwrap_or_default()
            })
            .await;

        if is_latest {
            self.get_at_heads(id, heads).await
        } else {
            Ok(None)
        }
    }

    /// Get a doc along with its current heads (for later patching)
    pub async fn get_with_heads(
        &self,
        doc_id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
    ) -> Res<Option<(Arc<Doc>, ChangeHashSet)>> {
        // latest head is stored in the drawer
        let Some(latest_heads) = self
            .store
            .query_sync(|store| {
                store
                    .map
                    .get(doc_id)
                    .and_then(|entry| entry.branches.get(&branch_path.to_string_lossy().to_string()).cloned())
            })
            .await
        else {
            return Ok(None);
        };
        let doc = self.get_at_heads(doc_id, &latest_heads).await?;
        Ok(doc.map(|d| (d, latest_heads)))
    }

    pub async fn get_at_heads(&self, id: &DocId, heads: &ChangeHashSet) -> Res<Option<Arc<Doc>>> {
        if let Some(cached) = self.cache.get(id) {
            if cached.1 == *heads {
                return Ok(Some(Arc::clone(&cached.0)));
            }
        }
        let Some(handle) = self.get_handle(id).await? else {
            return Ok(None);
        };
        let (doc, heads) = handle.with_document(move |am_doc| {
            let version = am_doc
                .fork_at(&heads)
                .wrap_err("error forking doc at heads")?;
            // Hydrate as automerge Doc, then convert to root Doc
            let doc: ThroughJson<Doc> =
                autosurgeon::hydrate(&version).wrap_err("error hydrating")?;
            eyre::Ok((doc.0, heads))
        })?;
        let doc: Arc<Doc> = Arc::new(doc);
        self.cache.insert(id.clone(), (doc.clone(), heads.clone()));
        Ok(Some(doc))
    }

    async fn get_actor_id_for_branch(&self, _branch_path: &daybook_types::doc::BranchPath) -> Res<automerge::ActorId> {
        Ok(automerge::ActorId::random())
    }

    pub async fn update_at_heads(
        &self,
        patch: DocPatch,
        branch_path: daybook_types::doc::BranchPath,
        heads: Option<ChangeHashSet>,
    ) -> Result<(), UpdateDocErr> {
        debug!(
            "update_at_heads start: id={}, branch={:?}",
            patch.id, branch_path
        );
        if self.cancel_token.is_cancelled() {
            return Err(UpdateDocErr::Other {
                inner: ferr!("repo is stopped"),
            });
        }
        if patch.is_empty() {
            return Ok(());
        }

        let Some(handle) = self.get_handle(&patch.id).await? else {
            return Err(UpdateDocErr::DocNotFound { id: patch.id });
        };
        let heads = match heads {
            Some(val) => val,
            None => match self
                .store
                .query_sync(|store| {
                    store
                        .map
                        .get(&patch.id)
                        .and_then(|entry| entry.branches.get(&branch_path.to_string_lossy().to_string()).cloned())
                })
                .await
            {
                Some(val) => val,
                None => return Err(UpdateDocErr::BranchNotFound { name: branch_path.to_string_lossy().to_string() }),
            },
        };

        let _actor_id = self.get_actor_id_for_branch(&branch_path).await?;

        let _actor_id = self.get_actor_id_for_branch(&branch_path).await?;

        let id = patch.id.clone();
        let prop_keys = patch.props_set.keys().cloned().collect::<Vec<_>>();
        let current_heads = self.current_heads();
        let new_heads = handle.with_document(|am_doc| {
            am_doc.set_actor(automerge::ActorId::random());
            let mut tx = am_doc.transaction_at(automerge::PatchLog::null(), &heads);

            let props_obj = match tx.get(automerge::ROOT, "props")? {
                Some((automerge::Value::Object(automerge::ObjType::Map), id)) => id,
                _ => tx.put_object(automerge::ROOT, "props", automerge::ObjType::Map)?,
            };

            for (key, value) in &patch.props_set {
                let key_str = key.to_string();
                autosurgeon::reconcile_prop(
                    &mut tx,
                    &props_obj,
                    key_str.as_str(),
                    &ThroughJson(value.clone()),
                )
                .map_err(|e| ferr!("error reconciling prop {}: {:?}", key, e))?;
            }
            for key in &patch.props_remove {
                let key_str = key.to_string();
                tx.delete(&props_obj, key_str.as_str())
                    .map_err(|e| ferr!("error deleting prop {}: {:?}", key, e))?;
            }

            if tx.pending_ops() > 0 {
                tx.put(automerge::ROOT, "updatedAt", Timestamp::now().to_string())?;
            }

            tx.commit();
            let new_heads = ChangeHashSet(am_doc.get_heads().into());

            // Invalidate cache
            self.cache.remove(&patch.id);

            eyre::Ok(new_heads)
        })?;

        let (_, drawer_heads) = self.store
            .mutate_sync(|store| {
                let entry = store
                    .map
                    .get_mut(&id)
                    .expect("doc handle found but no entry in store");

                for key in patch.props_remove {
                    entry.prop_blames.remove(&key.to_string());
                }
                for key in prop_keys {
                    entry.prop_blames.insert(
                        key.to_string(),
                        PropBlame {
                            heads: new_heads.clone(),
                        },
                    );
                }
                entry.branches.insert(branch_path.to_string_lossy().to_string(), new_heads);
                entry.version = Uuid::new_v4();
                entry.previous_version_heads = Some(current_heads);
                if let Some(user_path) = patch.user_path {
                    entry.users.insert(
                        self.local_actor_id.to_string(),
                        crate::config::UserMeta {
                            user_path,
                            seen_at: Timestamp::now(),
                        }
                        .into(),
                    );
                }
            })
            .await?;

        self.update_current_heads(ChangeHashSet(drawer_heads.into_iter().collect()));

        Ok(())
    }

    /// Apply a batch of patches to documents. Each patch is paired with its document id.
    pub async fn update_batch(&self, patches: Vec<UpdateDocArgs>) -> Result<(), UpdateDocBatchErr> {
        use futures::StreamExt;
        let mut stream = futures::stream::iter(patches.into_iter().enumerate().map(
            |(ii, args)| async move {
                self.update_at_heads(args.patch, args.branch_path, args.heads)
                    .await
                    .map_err(|err| (ii, err))
            },
        ))
        // FIXME: futurelock alert
        .buffer_unordered(16);
        let mut errors = HashMap::new();
        while let Some(res) = stream.next().await {
            if let Err((ii, err)) = res {
                errors.insert(ii as u64, err);
            }
        }
        if !errors.is_empty() {
            Err(UpdateDocBatchErr { map: errors })
        } else {
            Ok(())
        }
    }

    pub async fn merge_from_heads(
        &self,
        id: &DocId,
        to_branch: &daybook_types::doc::BranchPath,
        from_heads: &ChangeHashSet,
        user_path: Option<daybook_types::doc::UserPath>,
    ) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }

        let mutation_actor_id = if let Some(path) = &user_path {
            daybook_types::doc::user_path::to_actor_id(path)
        } else {
            self.local_actor_id.clone()
        };

        let Some(handle) = self.get_handle(id).await? else {
            eyre::bail!("doc not found: {id}");
        };

        let to_heads = self
            .store
            .query_sync(|store| {
                store
                    .map
                    .get(id)
                    .and_then(|entry| entry.branches.get(&to_branch.to_string_lossy().to_string()).cloned())
            })
            .await
            .ok_or_eyre(format!("branch not found: {:?}", to_branch))?;

        let (new_heads, modified_props, deleted_props) = handle.with_document(|am_doc| {
            let mut am_to = am_doc
                .fork_at(&to_heads.0)
                .wrap_err("error forking am_to")?;
            am_to.set_actor(mutation_actor_id);

            let mut am_from = am_doc
                .fork_at(&from_heads.0)
                .map_err(|e| ferr!("error forking am_from: {e}"))?;

            let mut patch_log = automerge::PatchLog::active();

            // Merge am_from into am_to
            am_to
                .merge_and_log_patches(&mut am_from, &mut patch_log)
                .wrap_err("error merging")?;

            let patches = am_to.make_patches(&mut patch_log);
            let new_heads_vec = am_to.get_heads();
            let new_heads = ChangeHashSet(new_heads_vec.clone().into());

            // Merge the result back into the main document handle
            am_doc.merge(&mut am_to).wrap_err("error merging back to am_doc")?;

            let mut modified_props = HashSet::new();
            let mut deleted_props = HashSet::new();

            for patch in patches {
                if patch.path.len() >= 2 && patch.path[0].1 == automerge::Prop::Map("props".into())
                {
                    if let automerge::Prop::Map(ref prop_key_str) = patch.path[1].1 {
                        match &patch.action {
                            automerge::PatchAction::DeleteMap { .. }
                            | automerge::PatchAction::DeleteSeq { .. } => {
                                deleted_props.insert(prop_key_str.to_string());
                            }
                            _ => {
                                modified_props.insert(prop_key_str.to_string());
                            }
                        }
                    }
                }
            }

            eyre::Ok((new_heads, modified_props, deleted_props))
        })?;

        let current_heads = self.current_heads();
        let (_, drawer_heads) = self.store
            .mutate_sync(|store| {
                let entry = store.map.get_mut(id).expect("doc entry disappeared");
                for prop_name in deleted_props {
                    entry.prop_blames.remove(&prop_name);
                }
                for prop_name in modified_props {
                    entry.prop_blames.insert(
                        prop_name,
                        PropBlame {
                            heads: new_heads.clone(),
                        },
                    );
                }
                entry.branches.insert(to_branch.to_string_lossy().to_string(), new_heads);
                entry.version = Uuid::new_v4();
                entry.previous_version_heads = Some(current_heads);
                if let Some(user_path) = user_path {
                    entry.users.insert(
                        self.local_actor_id.to_string(),
                        crate::config::UserMeta {
                            user_path,
                            seen_at: Timestamp::now(),
                        }
                        .into(),
                    );
                }
            })
            .await?;

        self.update_current_heads(ChangeHashSet(drawer_heads.into_iter().collect()));

        self.cache.remove(id);

        Ok(())
    }

    pub async fn merge_from_branch(
        &self,
        id: &DocId,
        to_branch: &daybook_types::doc::BranchPath,
        from_branch: &daybook_types::doc::BranchPath,
        user_path: Option<daybook_types::doc::UserPath>,
    ) -> Res<()> {
        let from_heads = self
            .store
            .query_sync(|store| {
                store
                    .map
                    .get(id)
                    .and_then(|entry| entry.branches.get(&from_branch.to_string_lossy().to_string()).cloned())
            })
            .await
            .ok_or_eyre(format!("from_branch not found: {:?}", from_branch))?;

        self.merge_from_heads(id, to_branch, &from_heads, user_path)
            .await
    }

    pub async fn delete_branch(
        &self,
        id: &DocId,
        branch_path: &daybook_types::doc::BranchPath,
        user_path: Option<daybook_types::doc::UserPath>,
    ) -> Res<bool> {
        let current_heads = self.current_heads();
        let (existed, drawer_heads) = self
            .store
            .mutate_sync(|store| {
                let Some(entry) = store.map.get_mut(id) else {
                    return false;
                };
                let existed = entry.branches.remove(&branch_path.to_string_lossy().to_string()).is_some();
                if existed {
                    entry.version = Uuid::new_v4();
                    entry.previous_version_heads = Some(current_heads);
                    if let Some(user_path) = user_path {
                        entry.users.insert(
                            self.local_actor_id.to_string(),
                            crate::config::UserMeta {
                                user_path,
                                seen_at: Timestamp::now(),
                            }
                            .into(),
                        );
                    }
                }
                existed
            })
            .await?;

        if existed {
            self.update_current_heads(ChangeHashSet(drawer_heads.into_iter().collect()));
        }
        Ok(existed)
    }

    // Delete: evict from drawer and cache (document remains in repo for now)
    pub async fn del(&self, id: &DocId) -> Res<bool> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let (existed, drawer_heads) = self
            .store
            .mutate_sync(|store| store.map.remove(id).is_some())
            .await?;
        let drawer_heads = ChangeHashSet(drawer_heads.into_iter().collect());

        if existed {
            self.update_current_heads(drawer_heads.clone());
        }

        self.cache.remove(id);
        self.handles.remove(id);
        if existed {
            self.registry
                .notify([DrawerEvent::ListChanged { drawer_heads }]);
        }
        Ok(existed)
    }
}

impl Repo for DrawerRepo {
    type Event = DrawerEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }
    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
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
        doc.put(ROOT, "$schema", "daybook.drawer")?;
        reconcile_prop(
            &mut doc,
            ROOT,
            super::DrawerStore::PROP,
            super::DrawerStore::default(),
        )?;
        Ok(doc.save_nocompress())
    }
}

mod tests {

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn smoke() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let (client_acx, client_acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "client".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;
        let (server_acx, server_acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "server".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        crate::tincans::connect_repos(&client_acx.repo(), &server_acx.repo());
        client_acx.repo().when_connected("server".into()).await?;
        server_acx.repo().when_connected("client".into()).await?;

        let drawer_doc_id = {
            let doc = automerge::Automerge::load(&version_updates::version_latest()?)?;
            let handle = client_acx.add_doc(doc).await?;
            handle.document_id().clone()
        };
        let (client_repo, client_stop) = DrawerRepo::load(
            client_acx.clone(),
            drawer_doc_id.clone(),
            automerge::ActorId::random(),
        )
        .await?;
        let (server_repo, server_stop) = DrawerRepo::load(
            server_acx.clone(),
            drawer_doc_id.clone(),
            automerge::ActorId::random(),
        )
        .await?;

        let (server_notif_tx, mut server_notif_rx) = tokio::sync::mpsc::unbounded_channel();
        let _listener_handle = server_repo
            .register_listener(move |msg| server_notif_tx.send(msg).expect(ERROR_CHANNEL));

        let (client_notif_tx, mut client_notif_rx) = tokio::sync::mpsc::unbounded_channel();
        let _client_listener_handle = client_repo
            .register_listener(move |msg| client_notif_tx.send(msg).expect(ERROR_CHANNEL));

        let new_doc_id = client_repo
            .add(AddDocArgs {
                branch_path: daybook_types::doc::BranchPath::from("main"),
                props: [
                    //
                    (
                        daybook_types::doc::DocPropKey::from(
                            daybook_types::doc::WellKnownPropTag::Content,
                        ),
                        daybook_types::doc::WellKnownProp::Content(
                            daybook_types::doc::DocContent::Text(
                                //
                                "Hello, world!".into(),
                            ),
                        )
                        .into(),
                    ),
                ]
                .into(),
                user_path: None,
            })
            .await?;

        {
            let event =
                tokio::time::timeout(std::time::Duration::from_secs(1), client_notif_rx.recv())
                    .await
                    .wrap_err("timeout")?
                    .ok_or_eyre("channel closed")?;
            match &*event {
                DrawerEvent::DocAdded { id, .. } => {
                    assert_eq!(*id, new_doc_id);
                }
                _ => eyre::bail!("unexpected event"),
            }
        }
        {
            let event =
                tokio::time::timeout(std::time::Duration::from_secs(1), server_notif_rx.recv())
                    .await
                    .wrap_err("timeout")?
                    .ok_or_eyre("channel closed")?;
            match &*event {
                DrawerEvent::DocAdded { id, .. } => {
                    assert_eq!(*id, new_doc_id);
                }
                _ => eyre::bail!("unexpected event"),
            }
        }

        client_acx_stop.stop().await?;
        server_acx_stop.stop().await?;
        client_stop.stop().await?;
        server_stop.stop().await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_merge_and_branches() -> Res<()> {
        use daybook_types::doc::{DocContent, DocPropKey, WellKnownProp, WellKnownPropTag};

        utils_rs::testing::setup_tracing_once();
        println!("Test starting...");
        let (acx, acx_stop) = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "test".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;

        let drawer_doc_id = {
            let doc = automerge::Automerge::load(&version_updates::version_latest()?)?;
            let handle = acx.add_doc(doc).await?;
            handle.document_id().clone()
        };
        println!("Drawer doc id: {}", drawer_doc_id);
        let (repo, repo_stop) =
            DrawerRepo::load(acx.clone(), drawer_doc_id, automerge::ActorId::random()).await?;

        // 1. Add doc on main
        let prop_title = DocPropKey::from(WellKnownPropTag::TitleGeneric);
        let prop_content = DocPropKey::from(WellKnownPropTag::Content);

        println!("Adding doc...");
        let doc_id = repo
            .add(AddDocArgs {
                branch_path: daybook_types::doc::BranchPath::from("main"),
                props: [(
                    prop_title.clone(),
                    WellKnownProp::TitleGeneric("Initial".into()).into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;
        println!("Doc added: {}", doc_id);

        // 2. Fork branch-a and branch-b from main by updating with explicit heads
        let main_heads = repo
            .store
            .query_sync(|s| {
                s.map
                    .get(&doc_id)
                    .unwrap()
                    .branches
                    .get(&daybook_types::doc::BranchPath::from("main").to_string_lossy().to_string())
                    .cloned()
                    .unwrap()
            })
            .await;
        println!("Main heads: {:?}", main_heads);

        // 3. Update branch-a (change title)
        println!("Updating branch-a...");
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                props_set: [(
                    prop_title.clone(),
                    WellKnownProp::TitleGeneric("Title A".into()).into(),
                )]
                .into(),
                props_remove: vec![],
                user_path: None,
            },
            "branch-a".into(),
            Some(main_heads.clone()),
        )
        .await?;
        println!("Branch-a updated.");

        // 4. Update branch-b (add content)
        println!("Updating branch-b...");
        repo.update_at_heads(
            DocPatch {
                id: doc_id.clone(),
                props_set: [(
                    prop_content.clone(),
                    WellKnownProp::Content(DocContent::Text("Content B".into())).into(),
                )]
                .into(),
                props_remove: vec![],
                user_path: None,
            },
            "branch-b".into(),
            Some(main_heads.clone()),
        )
        .await?;
        println!("Branch-b updated.");

        // 5. Merge branch-a to main
        println!("Merging branch-a to main...");
        repo.merge_from_branch(&doc_id, &daybook_types::doc::BranchPath::from("main"), &daybook_types::doc::BranchPath::from("branch-a"), None)
            .await?;
        println!("Branch-a merged.");

        // 6. Merge branch-b to main
        println!("Merging branch-b to main...");
        repo.merge_from_branch(&doc_id, &daybook_types::doc::BranchPath::from("main"), &daybook_types::doc::BranchPath::from("branch-b"), None)
            .await?;
        println!("Branch-b merged.");

        // 7. Assertions
        println!("Checking assertions...");
        let merged_doc = repo.get(&doc_id, &daybook_types::doc::BranchPath::from("main")).await?.unwrap();
        assert_eq!(
            merged_doc.props.get(&prop_title).unwrap(),
            &serde_json::Value::from(WellKnownProp::TitleGeneric("Title A".into()))
        );
        assert_eq!(
            merged_doc.props.get(&prop_content).unwrap(),
            &serde_json::Value::from(WellKnownProp::Content(DocContent::Text("Content B".into())))
        );

        let entry = repo
            .store
            .query_sync(|s| s.map.get(&doc_id).cloned().unwrap())
            .await;
        // Check prop blames
        assert!(entry.prop_blames.contains_key(&prop_title.to_string()));
        assert!(entry.prop_blames.contains_key(&prop_content.to_string()));

        // 8. Delete branch-a
        println!("Deleting branch-a...");
        assert!(repo.delete_branch(&doc_id, &daybook_types::doc::BranchPath::from("branch-a"), None).await?);
        let entry = repo
            .store
            .query_sync(|s| s.map.get(&doc_id).cloned().unwrap())
            .await;
        assert!(!entry.branches.contains_key(&daybook_types::doc::BranchPath::from("branch-a").to_string_lossy().to_string()));
        assert!(entry.branches.contains_key(&daybook_types::doc::BranchPath::from("branch-b").to_string_lossy().to_string()));
        assert!(entry.branches.contains_key(&daybook_types::doc::BranchPath::from("main").to_string_lossy().to_string()));

        println!("Test finished successfully!");
        acx_stop.stop().await?;
        repo_stop.stop().await?;
        Ok(())
    }

    #[test]
    fn test_automerge_merge_actor_ids() {
        use automerge::transaction::Transactable;
        use automerge::{ActorId, Automerge, ROOT};

        // 1. Create a base document with some data
        let mut doc1 = Automerge::new();
        let actor1 = ActorId::from(b"actor1");
        doc1.set_actor(actor1.clone());
        doc1.transact(|tx| tx.put(ROOT, "key1", "val1")).unwrap();
        let base_heads = doc1.get_heads();

        // 2. Fork doc1 into doc_to and doc_from at base_heads
        let mut doc_to = doc1.fork_at(&base_heads).unwrap();
        let mut doc_from = doc1.fork_at(&base_heads).unwrap();

        // Automerge fork_at actually generates a new random actor ID for the fork
        assert_ne!(doc_to.get_actor(), &actor1);
        assert_ne!(doc_from.get_actor(), &actor1);
        assert_ne!(doc_to.get_actor(), doc_from.get_actor());

        // 3. Make a change on doc_to
        doc_to.transact(|tx| tx.put(ROOT, "key_to", "val_to")).unwrap();

        // 4. Set a specific actor ID on doc_from to simulate a conflict if we used the same one
        // (Though Automerge forks already have unique ones, we might have manually set them)
        let random_actor = ActorId::random();
        doc_from.set_actor(random_actor.clone());
        doc_from.transact(|tx| tx.put(ROOT, "key_from", "val_from")).unwrap();

        // 5. Merge doc_from into doc_to
        doc_to.merge(&mut doc_from).unwrap();

        // 6. Confirm merge worked
        // confirm merge worked
        assert_eq!(
            doc_to.get(ROOT, "key_from").unwrap().unwrap().0.to_str().unwrap(),
            "val_from"
        );
        assert_eq!(
            doc_to.get(ROOT, "key_to").unwrap().unwrap().0.to_str().unwrap(),
            "val_to"
        );

        // 7. Additionally, confirm that if we fork and merge without making changes,
        // the random actor ID doesn't show up in any operations.
        let mut doc_target = doc1.fork_at(&base_heads).unwrap();
        let mut doc_source = doc1.fork_at(&base_heads).unwrap();
        let random_actor_2 = ActorId::random();
        doc_source.set_actor(random_actor_2.clone());
        
        // Merge without changes on doc_source
        doc_target.merge(&mut doc_source).unwrap();
        
        // Check all changes in doc_target
        for change in doc_target.get_changes(&[]) {
            assert_ne!(change.actor_id(), &random_actor_2, "Random actor ID should NOT show up in changes if no ops were made with it");
        }

        // 8. Confirm that using the SAME actor ID on both forks causes merge issues
        // (This is why we use random actor IDs in our implementation)
        let mut doc_a = doc1.fork_at(&base_heads).unwrap();
        let mut doc_b = doc1.fork_at(&base_heads).unwrap();
        
        let shared_actor = ActorId::random();
        doc_a.set_actor(shared_actor.clone());
        doc_b.set_actor(shared_actor.clone());
        
        doc_a.transact(|tx| tx.put(ROOT, "key_a", "val_a")).unwrap();
        doc_b.transact(|tx| tx.put(ROOT, "key_b", "val_b")).unwrap();
        
        // Merging doc_b into doc_a when they share an actor ID and have diverged
        // results in an error because of sequence number conflict.
        let res = doc_a.merge(&mut doc_b);
        assert!(res.is_err(), "Merge should fail when sharing actor ID and sequence numbers");
    }
}
