//! FIXME: make this more memory efficent
//! - Don't hold the Store in memory but hydrate as needed
//! - Use LRU cache for Doc cache

use crate::interlude::*;

use daybook_types::doc::{AddDocArgs, ChangeHashSet, Doc, DocId, DocPatch};
use tokio_util::sync::CancellationToken;
// Automerge types for hydrate/reconcile boundaries
// We use the conversion functions from daybook_types::automerge module
// The automerge::Doc type is accessed through conversions
use crate::repos::Repo;
use std::str::FromStr;

#[derive(Default, Debug, Reconcile, Hydrate)]
pub struct DrawerStore {
    pub map: HashMap<DocId, DocEntry>,
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
    acx: AmCtx,
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
    ListChanged,
    DocAdded {
        id: DocId,
        entry: DocEntry,
    },
    DocUpdated {
        id: DocId,
        branches_diffs: HashMap<String, HeadDiff>,
    },
    DocDeleted {
        id: DocId,
        entry: DocEntry, // old_heads: ChangeHashSet,
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
    pub async fn load(
        acx: AmCtx,
        drawer_doc_id: DocumentId,
    ) -> Res<(Arc<Self>, crate::repos::RepoStopToken)> {
        let registry = crate::repos::ListenersRegistry::new();

        let store = DrawerStore::load(&acx, &drawer_doc_id).await?;
        let store = crate::stores::StoreHandle::new(store, acx.clone(), drawer_doc_id.clone());

        let (broker, broker_stop) = {
            let handle = acx
                .find_doc(&drawer_doc_id)
                .await?
                .expect("doc should have been loaded");
            acx.change_manager().add_doc(handle).await?
        };

        let (notif_tx, notif_rx) = tokio::sync::mpsc::unbounded_channel::<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >();
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
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >,
        cancel_token: CancellationToken,
    ) -> Res<()> {
        // FIXME: this code doesn't seem right and has missing features

        // let mut added_docs = std::collections::HashSet::new();
        // let mut updated_docs = std::collections::HashSet::new();
        // let mut deleted_docs = std::collections::HashSet::new();
        loop {
            let notifs = tokio::select! {
                biased;
                _ = cancel_token.cancelled() => {
                    // Try to drain remaining notifications
                    while let Ok(notifs) = notif_rx.try_recv() {
                        self.process_notifs(notifs).await?;
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
            self.process_notifs(notifs).await?;
        }
        // Notify repo listeners that the docs list changed
        self.registry.notify(DrawerEvent::ListChanged);
        Ok(())
    }

    async fn process_notifs(
        &self,
        notifs: Vec<utils_rs::am::changes::ChangeNotification>,
    ) -> Res<()> {
        for notif in notifs {
            if utils_rs::am::changes::path_matches(
                &[DrawerStore::PROP.into(), "map".into()],
                &notif.patch.path,
            ) {
                match &notif.patch.action {
                    automerge::PatchAction::PutMap {
                        key: new_doc_id,
                        value: (_val, obj_id),
                        ..
                    } => {
                        let mut new_entry = self
                            .acx
                            .hydrate_path_at_heads::<DocEntry>(
                                &self.drawer_doc_id,
                                &notif.heads,
                                obj_id.clone(),
                                vec![],
                            )
                            .await
                            .expect("error hydrating doc entry")
                            .expect("schema violation");

                        let old_entry = self
                            .store
                            .mutate_sync({
                                let key = new_doc_id.clone();
                                |store| store.map.insert(key, new_entry.clone())
                            })
                            .await?;
                        if let Some(old_entry) = old_entry {
                            let mut branch_diffs: HashMap<_, _> = old_entry
                                .branches
                                .into_iter()
                                .filter_map(|(key, old_hash)| {
                                    match new_entry.branches.remove(&key) {
                                        Some(new_hash) if new_hash == old_hash => None,
                                        Some(new_hash) => Some((
                                            key,
                                            HeadDiff {
                                                from: Some(old_hash),
                                                to: Some(new_hash),
                                            },
                                        )),
                                        None => Some((
                                            key,
                                            HeadDiff {
                                                from: Some(old_hash),
                                                to: None,
                                            },
                                        )),
                                    }
                                })
                                .collect();
                            for (key, val) in new_entry.branches {
                                branch_diffs.insert(
                                    key,
                                    HeadDiff {
                                        from: None,
                                        to: Some(val),
                                    },
                                );
                            }
                            self.registry.notify(DrawerEvent::DocUpdated {
                                id: new_doc_id.clone(),
                                branches_diffs: branch_diffs,
                            })
                        } else {
                            self.registry.notify(DrawerEvent::DocAdded {
                                id: new_doc_id.clone(),
                                entry: new_entry,
                            })
                        }
                    }
                    automerge::PatchAction::DeleteMap { key } => {
                        let old_entry = self
                            .store
                            .mutate_sync(|store| store.map.remove(key))
                            .await?;
                        if let Some(old_entry) = old_entry {
                            self.registry.notify(DrawerEvent::DocDeleted {
                                id: key.clone(),
                                entry: old_entry,
                            })
                        }
                    }
                    _ => {
                        info!(?notif.patch, "XXX weird patch action");
                    }
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

        let (new_doc, heads) = handle
            .with_document(move |doc_am| {
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
            })
            .wrap_err(ERROR_TOKIO)?;
        let new_doc = Arc::new(new_doc);
        let heads = ChangeHashSet(heads.into());
        let entry = DocEntry {
            branches: [(args.branch_name, heads.clone())].into(),
        };

        // store id in drawer AM
        self.store
            .mutate_sync(|store| {
                store.map.insert(new_doc.id.clone(), entry.clone());
            })
            .await?;

        // cache the handle under the doc's Uuid id
        let out_id = new_doc.id.clone();
        self.cache
            .insert(new_doc.id.clone(), (new_doc, heads.clone()));
        self.handles.insert(out_id.clone(), handle);
        self.registry.notify(DrawerEvent::DocAdded {
            id: out_id.clone(),
            entry,
        });
        self.registry.notify(DrawerEvent::ListChanged);
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

    // Get a Doc by id by hydrating its automerge document
    pub async fn get(&self, id: &DocId, branch_name: &str) -> Res<Option<Arc<Doc>>> {
        self.get_with_heads(id, branch_name)
            .await
            .map(|opt| opt.map(|(doc, _)| doc))
    }

    pub async fn get_doc_branches(&self, id: &DocId) -> Option<DocNBranches> {
        self.store
            .query_sync(|store| {
                store.map.get(id).map(|entry| DocNBranches {
                    doc_id: id.clone(),
                    branches: entry.branches.clone(),
                })
            })
            .await
    }

    /// Get a doc along with its current heads (for later patching)
    pub async fn get_with_heads(
        &self,
        id: &DocId,
        branch_name: &str,
    ) -> Res<Option<(Arc<Doc>, ChangeHashSet)>> {
        // latest head is stored in the drawer
        let Some(latest_heads) = self
            .store
            .query_sync(|store| {
                store
                    .map
                    .get(id)
                    .and_then(|entry| entry.branches.get(branch_name).cloned())
            })
            .await
        else {
            return Ok(None);
        };
        let doc = self.get_at_heads(id, &latest_heads).await?;
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
                        .map
                        .get(&patch.id)
                        .and_then(|entry| entry.branches.get(&branch_name).cloned())
                })
                .await
            {
                Some(val) => val,
                None => return Err(UpdateDocErr::BranchNotFound { name: branch_name }),
            },
        };

        let id = patch.id.clone();
        let new_heads = handle
            .with_document(|am_doc| {
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
            })
            .wrap_err(ERROR_TOKIO)?;

        self.store
            .mutate_sync(|store| {
                let entry = store
                    .map
                    .get_mut(&id)
                    .expect("doc handle found but no entry in store");
                let _old_heads = entry.branches.insert(branch_name, new_heads);
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
        let existed = self
            .store
            .mutate_sync(|store| store.map.remove(id).is_some())
            .await?;
        self.cache.remove(id);
        self.handles.remove(id);
        if existed {
            self.registry.notify(DrawerEvent::ListChanged);
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
