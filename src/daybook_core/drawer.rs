//! FIXME: make this more memory efficent
//! - Don't hold the Store in memory but hydrate as needed
//! - Use LRU cache for Doc cache

use crate::interlude::*;

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
    pub map: HashMap<Uuid, DocEntry>,
    // WARN: field ordering is imporant here, we want reconciliation
    // to create changes on the map before the atomic map so that changes
    // to the atmoic map will be always observed after the corresponding
    // map change
    // FIXME: use bs58 repr for uuids for efficency
    pub atomic_map: HashMap<DocId, Uuid>,
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct DocEntry {
    pub branches: HashMap<String, ChangeHashSet>,
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct DocNBranches {
    pub doc_id: DocId,
    pub branches: HashMap<String, ChangeHashSet>,
}

impl DocNBranches {
    pub fn main_branch_name(&self) -> Option<&str> {
        if self.branches.contains_key("main") {
            Some("main")
        } else {
            match self.branches.keys().next() {
                Some(val) => Some(val),
                None => None,
            }
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
    cancel_token: CancellationToken,
    _change_listener_tickets: Vec<utils_rs::am::changes::ChangeListenerRegistration>,
    drawer_am_handle: samod::DocHandle,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct HeadDiff {
    pub from: Option<ChangeHashSet>,
    pub to: Option<ChangeHashSet>,
}

// Minimal event enum so Kotlin can refresh via ffiList on changes
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DrawerEvent {
    ListChanged {
        drawer_commit: ChangeHashSet,
    },
    DocAdded {
        id: DocId,
        entry: DocEntry,
        drawer_commit: ChangeHashSet,
    },
    DocUpdated {
        id: DocId,
        branches_diffs: HashMap<String, HeadDiff>,
        drawer_commit: ChangeHashSet,
    },
    DocDeleted {
        id: DocId,
        entry: DocEntry, // old_heads: ChangeHashSet,
        drawer_commit: ChangeHashSet,
    },
}

#[derive(Debug)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct UpdateDocArgs {
    branch_name: String,
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
    const ERROR_INCONSISTENT: &str = "inconsistent drawer state";
    const ERROR_SCHEMA: &str = "schema violation";

    pub async fn load(
        acx: AmCtx,
        drawer_doc_id: DocumentId,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let registry = crate::repos::ListenersRegistry::new();

        let store = DrawerStore::load(&acx, &drawer_doc_id).await?;
        let store = crate::stores::StoreHandle::new(store, acx.clone(), drawer_doc_id.clone());

        let drawer_am_handle = acx
            .find_doc(&drawer_doc_id)
            .await?
            .expect("doc should have been loaded");
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
            registry: Arc::clone(&registry),
            handles: default(),
            cache: default(),
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
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<ChangeNotification>>,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        // FIXME: this code doesn't seem right and has missing features
        let mut events = vec![];
        loop {
            let notifs = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    // Try to drain remaining notifications
                    while let Ok(notifs) = notif_rx.try_recv() {
                        // FIXME: dubious?
                        let mut last_heads = None;
                        for notif in notifs {
                            last_heads = Some(notif.heads.clone());
                            self.events_for_patch(&notif.patch, &notif.heads, &mut events).await?;
                        }
                        if !events.is_empty() {
                            let drawer_commit = ChangeHashSet(last_heads.expect("notifs not empty"));
                            self.registry.notify(
                                events
                                    .drain(..)
                                    .chain(std::iter::once(DrawerEvent::ListChanged { drawer_commit })),
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
                self.events_for_patch(&notif.patch, &notif.heads, &mut events).await?;
            }
            if !events.is_empty() {
                let drawer_commit = ChangeHashSet(last_heads.expect("notifs not empty"));
                self.registry.notify(
                    events
                        .drain(..)
                        .chain(std::iter::once(DrawerEvent::ListChanged { drawer_commit })),
                );
            }
        }
        Ok(())
    }

    /// assumes a patches are ordered in time ascendingly
    async fn events_for_patch(
        &self,
        patch: &automerge::Patch,
        patch_heads: &Arc<[automerge::ChangeHash]>,
        out: &mut Vec<DrawerEvent>,
    ) -> Res<()> {
        if utils_rs::am::changes::path_prefix_matches(
            &[DrawerStore::PROP.into(), "atomic_map".into()],
            &patch.path,
        ) {
            if patch.path.len() == 2 {
                panic!("whole atomic_map was replaced, unsupported!");
            }
            let Some((map_obj, automerge::Prop::Map(doc_id))) = patch.path.get(2) else {
                panic!("unexpected drawer structure")
            };
            let drawer_commit = ChangeHashSet(patch_heads.clone());
            match &patch.action {
                automerge::PatchAction::DeleteMap { .. } if *map_obj == patch.obj => {
                    let (old_entry, _) = self
                        .store
                        .mutate_sync(|store| {
                            let atomic_id = store.atomic_map.remove(doc_id)?;
                            Some(
                                store
                                    .map
                                    .remove(&atomic_id)
                                    .expect(Self::ERROR_INCONSISTENT),
                            )
                        })
                        .await?;
                    if let Some(old_entry) = old_entry {
                        out.push(DrawerEvent::DocDeleted {
                            id: doc_id.clone(),
                            entry: old_entry,
                            drawer_commit,
                        });
                    } else {
                        warn!(?doc_id, "delete on unknown drawer entry");
                    }
                }
                automerge::PatchAction::PutMap {
                    value: (_val, _obj_id),
                    conflict: true,
                    key: _,
                } if *map_obj == patch.obj => {
                    // TODO: two atomic maps are conflicting, we'll need to load
                    // both entries and create a new entry that contains the bran
                    todo!("conflict on doc_id")
                }
                automerge::PatchAction::PutMap {
                    value: (val, obj_id),
                    key,
                    ..
                } if *map_obj == patch.obj => {
                    debug_assert_eq!(key, doc_id);

                    let atomic_id_str = val.clone().into_string().expect(Self::ERROR_SCHEMA);

                    let Some(change_hash) = self
                        .drawer_am_handle
                        .with_document(|am_doc| am_doc.hash_for_opid(obj_id))
                    else {
                        eyre::bail!("patch not recognized by document");
                    };

                    let atomic_id: Uuid = atomic_id_str.parse().expect(Self::ERROR_SCHEMA);

                    let (new_entry, _) = self
                        .acx
                        .hydrate_path_at_heads::<DocEntry>(
                            &self.drawer_doc_id,
                            &[change_hash],
                            obj_id.clone(),
                            vec![
                                DrawerStore::PROP.into(),
                                "atomic_map".into(),
                                autosurgeon::Prop::Key(atomic_id_str.into()),
                            ],
                        )
                        .await
                        .expect("error hydrating doc entry")
                        .expect(Self::ERROR_SCHEMA);

                    let new_entry_copy = new_entry.clone();

                    let (old_entry, _) = self
                        .store
                        .mutate_sync({
                            |store| {
                                let old = store.map.insert(atomic_id.clone(), new_entry_copy);
                                assert!(old.is_none(), "fishy");
                                let old_atmoic_id =
                                    store.atomic_map.insert(doc_id.clone(), atomic_id)?;
                                store.map.remove(&old_atmoic_id)
                            }
                        })
                        .await?;

                    if let Some(old_entry) = old_entry {
                        let mut branch_diffs: HashMap<_, _> = old_entry
                            .branches
                            .into_iter()
                            .filter_map(|(key, old_hash)| match new_entry.branches.get(&key) {
                                Some(new_hash) if *new_hash == old_hash => None,
                                Some(new_hash) => Some((
                                    key,
                                    HeadDiff {
                                        from: Some(old_hash),
                                        to: Some(new_hash.clone()),
                                    },
                                )),
                                None => Some((
                                    key,
                                    HeadDiff {
                                        from: Some(old_hash),
                                        to: None,
                                    },
                                )),
                            })
                            .collect();
                        for (key, val) in &new_entry.branches {
                            if !branch_diffs.contains_key(key) {
                                branch_diffs.insert(
                                    key.clone(),
                                    HeadDiff {
                                        from: None,
                                        to: Some(val.clone()),
                                    },
                                );
                            }
                        }
                        out.push(DrawerEvent::DocUpdated {
                            id: doc_id.clone(),
                            branches_diffs: branch_diffs,
                            drawer_commit,
                        })
                    } else {
                        out.push(DrawerEvent::DocAdded {
                            id: doc_id.clone(),
                            entry: new_entry,
                            drawer_commit,
                        })
                    }
                }
                _ => {
                    panic!("unexpected drawer patch {patch:?}")
                }
            }
        }
        Ok(())
    }

    // NOTE: old contains/insert/remove removed. Use add/get/update/del instead.

    pub async fn list(&self) -> Vec<DocNBranches> {
        self.store
            .query_sync(|store| {
                store
                    .atomic_map
                    .iter()
                    .map(|(doc_id, atomic_id)| {
                        let entry = store.map.get(atomic_id).expect(Self::ERROR_INCONSISTENT);
                        DocNBranches {
                            doc_id: doc_id.clone(),
                            branches: entry.branches.clone(),
                        }
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

    pub async fn add(&self, args: AddDocArgs) -> Res<DocId> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        // Use AutoCommit for reconciliation
        let handle = self.acx.add_doc(automerge::Automerge::new()).await?;

        let new_doc = Doc {
            id: handle.document_id().to_string(),
            created_at: Timestamp::now(),
            updated_at: Timestamp::now(),
            props: args.props,
        };

        let (new_doc, heads) = handle.with_document(move |doc_am| {
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
        let entry = DocEntry {
            branches: [(args.branch_name, heads.clone())].into(),
        };

        // store id in drawer AM
        let (_, drawer_commit) = self.store
            .mutate_sync(|store| {
                let atomic_id = Uuid::new_v4();
                let old = store.map.insert(atomic_id.clone(), entry.clone());
                assert!(old.is_none(), "fishy");
                let old = store.atomic_map.insert(new_doc.id.clone(), atomic_id);
                assert!(old.is_none(), "fishy");
            })
            .await?;
        let drawer_commit = ChangeHashSet(drawer_commit.into_iter().collect());

        // cache the handle under the doc's Uuid id
        let out_id = new_doc.id.clone();
        self.cache
            .insert(new_doc.id.clone(), (new_doc, heads.clone()));
        self.handles.insert(out_id.clone(), handle);
        self.registry.notify([
            DrawerEvent::DocAdded {
                id: out_id.clone(),
                entry,
                drawer_commit: drawer_commit.clone(),
            },
            DrawerEvent::ListChanged {
                drawer_commit,
            },
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
                    .query_sync(|store| store.atomic_map.contains_key(id))
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

    pub async fn get(&self, id: &DocId, branch_name: &str) -> Res<Option<Arc<Doc>>> {
        self.get_with_heads(id, branch_name)
            .await
            .map(|opt| opt.map(|(doc, _)| doc))
    }

    pub async fn get_doc_branches(&self, doc_id: &DocId) -> Option<DocNBranches> {
        self.store
            .query_sync(|store| {
                store.atomic_map.get(doc_id).map(|atomic_id| {
                    let entry = store.map.get(atomic_id).expect(Self::ERROR_INCONSISTENT);
                    DocNBranches {
                        doc_id: doc_id.clone(),
                        branches: entry.branches.clone(),
                    }
                })
            })
            .await
    }

    pub async fn get_if_latest(
        &self,
        id: &DocId,
        branch_name: &str,
        heads: &ChangeHashSet,
    ) -> Res<Option<Arc<Doc>>> {
        let is_latest = self
            .store
            .query_sync(|store| {
                store
                    .atomic_map
                    .get(id)
                    .map(|atomic_id| store.map.get(atomic_id).expect(Self::ERROR_INCONSISTENT))
                    .and_then(|entry| entry.branches.get(branch_name))
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
        branch_name: &str,
    ) -> Res<Option<(Arc<Doc>, ChangeHashSet)>> {
        // latest head is stored in the drawer
        let Some(latest_heads) = self
            .store
            .query_sync(|store| {
                store
                    .atomic_map
                    .get(doc_id)
                    .map(|atomic_id| store.map.get(atomic_id).expect(Self::ERROR_INCONSISTENT))
                    .and_then(|entry| entry.branches.get(branch_name).cloned())
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

    pub async fn update_at_heads(
        &self,
        mut patch: DocPatch,
        branch_name: String,
        heads: Option<ChangeHashSet>,
    ) -> Result<(), UpdateDocErr> {
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
                        .atomic_map
                        .get(&patch.id)
                        .map(|atomic_id| store.map.get(atomic_id).expect(Self::ERROR_INCONSISTENT))
                        .and_then(|entry| entry.branches.get(&branch_name).cloned())
                })
                .await
            {
                Some(val) => val,
                None => return Err(UpdateDocErr::BranchNotFound { name: branch_name }),
            },
        };

        let id = patch.id.clone();
        let new_heads = handle.with_document(|am_doc| {
            let mut tx = am_doc.transaction_at(automerge::PatchLog::null(), &heads);

            let new_heads = match self.cache.get_mut(&patch.id) {
                // if the cached doc is at the head we're
                // looking for
                Some(mut entry) if entry.1 == heads => {
                    let mut doc = (*entry.0).clone();
                    patch.apply(&mut doc);
                    doc.updated_at = Timestamp::now();

                    let doc = ThroughJson(doc);
                    autosurgeon::reconcile(&mut tx, &doc).wrap_err("error reconciling")?;
                    tx.commit();
                    let doc = doc.0;

                    let heads = ChangeHashSet(am_doc.get_heads().into());
                    entry.0 = Arc::new(doc);
                    entry.1 = heads.clone();
                    eyre::Ok(heads)
                }
                _ => {
                    // Hydrate as automerge Doc, then convert to root Doc
                    let mut doc: ThroughJson<Doc> =
                        autosurgeon::hydrate(&tx).wrap_err("error hydrating")?;
                    patch.apply(&mut doc);

                    doc.updated_at = Timestamp::now();

                    autosurgeon::reconcile(&mut tx, &doc).wrap_err("error reconciling")?;
                    tx.commit();

                    let doc = doc.0;
                    let doc = Arc::new(doc);
                    let heads = ChangeHashSet(am_doc.get_heads().into());
                    self.cache.insert(patch.id.clone(), (doc, heads.clone()));
                    eyre::Ok(heads)
                }
            }?;
            eyre::Ok(new_heads)
        })?;

        self.store
            .mutate_sync(|store| {
                let atomic_id = store
                    .atomic_map
                    .get(&id)
                    .expect("doc handle found but no entry in store");
                let mut old_entry = store.map.remove(atomic_id).expect(Self::ERROR_INCONSISTENT);
                let _old_heads = old_entry.branches.insert(branch_name, new_heads);

                let atomic_id = Uuid::new_v4();
                let old = store.map.insert(atomic_id.clone(), old_entry);
                assert!(old.is_none(), "fishy");
                store.atomic_map.insert(id, atomic_id);
            })
            .await?;

        Ok(())
    }

    /// Apply a batch of patches to documents. Each patch is paired with its document id.
    pub async fn update_batch(&self, patches: Vec<UpdateDocArgs>) -> Result<(), UpdateDocBatchErr> {
        use futures::StreamExt;
        let mut stream = futures::stream::iter(patches.into_iter().enumerate().map(
            |(ii, args)| async move {
                self.update_at_heads(args.patch, args.branch_name, args.heads)
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

    // Delete: evict from drawer and cache (document remains in repo for now)
    pub async fn del(&self, id: &DocId) -> Res<bool> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is stopped");
        }
        let (existed, drawer_commit) = self
            .store
            .mutate_sync(|store| {
                store
                    .atomic_map
                    .remove(id)
                    .map(|atomic_id| {
                        store
                            .map
                            .remove(&atomic_id)
                            .expect(Self::ERROR_INCONSISTENT)
                    })
                    .is_some()
            })
            .await?;
        let drawer_commit = ChangeHashSet(drawer_commit.into_iter().collect());
        self.cache.remove(id);
        self.handles.remove(id);
        if existed {
            self.registry.notify([DrawerEvent::ListChanged { drawer_commit }]);
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
        utils_rs::testing::setup_tracing()?;
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
        let (client_repo, client_stop) =
            DrawerRepo::load(client_acx.clone(), drawer_doc_id.clone()).await?;
        let (server_repo, server_stop) =
            DrawerRepo::load(server_acx.clone(), drawer_doc_id.clone()).await?;

        let (server_notif_tx, mut server_notif_rx) = tokio::sync::mpsc::unbounded_channel();
        let _listener_handle = server_repo
            .register_listener(move |msg| server_notif_tx.send(msg).expect(ERROR_CHANNEL));

        let (client_notif_tx, mut client_notif_rx) = tokio::sync::mpsc::unbounded_channel();
        let _client_listener_handle = client_repo
            .register_listener(move |msg| client_notif_tx.send(msg).expect(ERROR_CHANNEL));

        let new_doc_id = client_repo
            .add(AddDocArgs {
                branch_name: "main".into(),
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
}
