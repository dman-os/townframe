use crate::{interlude::*, ChangeHashSet};

use crate::gen::doc::{Doc, DocId, DocPatch};
use crate::repos::Repo;
use std::str::FromStr;

#[derive(Default, Reconcile, Hydrate)]
pub struct DrawerStore {
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    map: HashMap<DocId, ChangeHashSet>,
}

impl DrawerStore {
    // const PATH: &[&str] = &["docs"];
    pub const PROP: &str = "docs";

    async fn load(acx: &AmCtx, drawer_doc_id: &DocumentId) -> Res<Self> {
        acx.hydrate_path::<Self>(drawer_doc_id, automerge::ROOT, vec![Self::PROP.into()])
            .await?
            .ok_or_eyre("unable to find obj in am_docc")
    }

    async fn register_change_listener<F>(
        acx: &AmCtx,
        broker: &utils_rs::am::changes::DocChangeBroker,
        mut path: Vec<autosurgeon::Prop<'static>>,
        on_change: F,
    ) -> Res<()>
    where
        F: Fn(Vec<utils_rs::am::changes::ChangeNotification>) + Send + Sync + 'static,
    {
        path.insert(0, Self::PROP.into());
        acx.change_manager()
            .add_listener(
                utils_rs::am::changes::ChangeFilter {
                    doc_id: Some(broker.filter()),
                    path,
                },
                on_change,
            )
            .await;

        Ok(())
    }
}

#[async_trait]
impl crate::stores::Store for DrawerStore {
    type FlushArgs = (AmCtx, DocumentId);

    async fn flush(&mut self, (acx, app_doc_id): &mut Self::FlushArgs) -> Res<()> {
        acx.reconcile_prop(app_doc_id, automerge::ROOT, Self::PROP, self)
            .await
    }
}

pub struct DrawerRepo {
    // drawer_doc_id: DocumentId,
    acx: AmCtx,
    store: crate::stores::StoreHandle<DrawerStore>,
    // in-memory cache of document handles
    handles: Arc<DHashMap<DocId, samod::DocHandle>>,
    cache: Arc<DHashMap<DocId, (Arc<Doc>, ChangeHashSet)>>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    _broker: Arc<utils_rs::am::changes::DocChangeBroker>,
    drawer_doc_id: DocumentId,
}

// Minimal event enum so Kotlin can refresh via ffiList on changes
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DrawerEvent {
    ListChanged,
    DocAdded {
        id: DocId,
        heads: ChangeHashSet,
    },
    DocUpdated {
        id: DocId,
        new_heads: ChangeHashSet,
        old_heads: ChangeHashSet,
    },
    DocDeleted {
        id: DocId,
        old_heads: ChangeHashSet,
    },
}

pub enum DrawerUpdate {}

impl DrawerRepo {
    pub async fn load(acx: AmCtx, drawer_doc_id: DocumentId) -> Res<Arc<Self>> {
        let registry = crate::repos::ListenersRegistry::new();

        let store = DrawerStore::load(&acx, &drawer_doc_id).await?;
        let store = crate::stores::StoreHandle::new(store, (acx.clone(), drawer_doc_id.clone()));

        let broker = {
            let handle = acx
                .find_doc(&drawer_doc_id)
                .await?
                .expect("doc should have been loaded");
            acx.change_manager().add_doc(handle)
        };

        let (notif_tx, notif_rx) = tokio::sync::mpsc::unbounded_channel::<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >();
        DrawerStore::register_change_listener(&acx, &broker, vec!["map".into()], {
            move |notifs| notif_tx.send(notifs).expect(ERROR_CHANNEL)
        })
        .await?;

        let repo = Self {
            acx,
            drawer_doc_id,
            store,
            registry: Arc::clone(&registry),
            handles: default(),
            cache: default(),
            _broker: broker,
        };
        let repo = Arc::new(repo);

        let _notif_worker = tokio::spawn({
            let repo = Arc::clone(&repo);
            async move { repo.handle_notifs(notif_rx).await }
        });

        Ok(repo)
    }

    async fn handle_notifs(
        self: &Self,
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >,
    ) -> Res<()> {
        // let mut added_docs = std::collections::HashSet::new();
        // let mut updated_docs = std::collections::HashSet::new();
        // let mut deleted_docs = std::collections::HashSet::new();
        while let Some(notifs) = notif_rx.recv().await {
            // added_docs.clear();
            // updated_docs.clear();
            // deleted_docs.clear();
            for notif in notifs {
                if utils_rs::am::changes::path_matches(
                    &[DrawerStore::PROP.into(), "map".into()],
                    &notif.patch.path,
                ) {
                    match &notif.patch.action {
                        automerge::PatchAction::PutMap {
                            key: new_doc_id,
                            value: (val, obj_id),
                            ..
                        } => {
                            let Some(automerge::ObjType::List) = val.to_objtype() else {
                                panic!("schema violation");
                            };

                            let new_heads = self
                                .acx
                                .hydrate_path_at_heads::<ChangeHashSet>(
                                    &self.drawer_doc_id,
                                    &notif.heads,
                                    obj_id.clone(),
                                    vec![],
                                )
                                .await
                                .expect("error hydrating at head")
                                .expect("schema violation");

                            let old_heads = self
                                .store
                                .mutate_sync({
                                    let key = new_doc_id.clone();
                                    |store| store.map.insert(key, new_heads.clone())
                                })
                                .await?;
                            if let Some(old_heads) = old_heads {
                                self.registry.notify(DrawerEvent::DocUpdated {
                                    id: new_doc_id.clone(),
                                    new_heads,
                                    old_heads,
                                })
                            } else {
                                self.registry.notify(DrawerEvent::DocAdded {
                                    id: new_doc_id.clone(),
                                    heads: new_heads,
                                })
                            }
                        }
                        automerge::PatchAction::DeleteMap { key } => {
                            let old_heads = self
                                .store
                                .mutate_sync(|store| store.map.remove(key))
                                .await?;
                            if let Some(old_heads) = old_heads {
                                self.registry.notify(DrawerEvent::DocDeleted {
                                    id: key.clone(),
                                    old_heads,
                                })
                            }
                        }
                        _ => {
                            info!(?notif.patch, "XXX weird patch action");
                        }
                    }
                }
            }
        }
        // Notify repo listeners that the docs list changed
        self.registry.notify(DrawerEvent::ListChanged);
        Ok(())
    }

    // NOTE: old contains/insert/remove removed. Use add/get/update/del instead.

    pub async fn list(&self) -> Vec<DocId> {
        self.store
            .query_sync(|store| store.map.keys().cloned().collect())
            .await
    }

    // Create a new doc (Automerge), reconcile the provided `Doc` into it, store and cache handle,
    // and add its id to the drawer set.
    pub async fn add(&self, mut new_doc: Doc) -> Res<DocId> {
        // Use AutoCommit for reconciliation
        let handle = self.acx.add_doc(automerge::Automerge::new()).await?;

        new_doc.id = handle.document_id().to_string();

        let (new_doc, heads) = tokio::task::spawn_blocking({
            let handle = handle.clone();
            move || {
                handle.with_document(move |doc_am| {
                    let doc = doc_am
                        .transact(move |tx| {
                            use automerge::transaction::Transactable;
                            tx.put(automerge::ROOT, "$schema", "daybook.doc")?;
                            autosurgeon::reconcile(tx, &new_doc)
                                .map_err(|err| ferr!(err.to_string()))
                                .wrap_err("error reconciling new doc")?;
                            eyre::Ok(new_doc)
                        })
                        .map(|val| val.result)
                        .map_err(|err| err.error)?;
                    eyre::Ok((doc, doc_am.get_heads()))
                })
            }
        })
        .await
        .wrap_err(ERROR_TOKIO)??;
        let new_doc = Arc::new(new_doc);
        let heads = ChangeHashSet(heads.into());

        // store id in drawer AM
        self.store
            .mutate_sync(|store| store.map.insert(new_doc.id.clone(), heads.clone()))
            .await?;

        // cache the handle under the doc's Uuid id
        let out_id = new_doc.id.clone();
        self.cache
            .insert(new_doc.id.clone(), (new_doc, heads.clone()));
        self.handles.insert(out_id.clone(), handle);
        self.registry.notify(DrawerEvent::DocAdded {
            id: out_id.clone(),
            heads,
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
    pub async fn get(&self, id: &DocId) -> Res<Option<Arc<Doc>>> {
        // latest head is stored in the drawer
        let Some(latest_heads) = self
            .store
            .query_sync(|store| store.map.get(id).cloned())
            .await
        else {
            return Ok(None);
        };
        self.get_at_heads(id, &latest_heads).await
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
        let (doc, heads) = tokio::task::block_in_place(move || {
            handle.with_document(move |doc| {
                let version = doc.fork_at(&heads).wrap_err("error forking doc at heads")?;
                let value: Doc = autosurgeon::hydrate(&version).wrap_err("error hydrating")?;
                eyre::Ok((value, heads))
            })
        })?;
        let doc = Arc::new(doc);
        self.cache.insert(id.clone(), (doc.clone(), heads.clone()));

        Ok(Some(doc))
    }

    async fn update(&self, mut patch: DocPatch) -> Res<()> {
        use struct_patch::Status;
        if patch.is_empty() {
            return Ok(());
        }
        let Some(id) = patch.id.take() else {
            eyre::bail!("patch has no id set");
        };
        let Some(handle) = self.get_handle(&id).await? else {
            eyre::bail!("patch for unknown document for id {id})");
        };
        let cache = self.cache.clone();
        tokio::task::spawn_blocking(move || {
            handle
                .with_document(move |am_doc| {
                    am_doc.transact(move |tx| {
                        match cache.get_mut(&id) {
                            Some(mut entry) => {
                                let mut doc = (*entry.0).clone();
                                doc.apply(patch);
                                autosurgeon::reconcile(tx, &doc).wrap_err("error reconciling")?;
                                entry.0 = Arc::new(doc);
                            }
                            None => {
                                let mut doc: Doc =
                                    autosurgeon::hydrate(tx).wrap_err("error hydrating")?;
                                doc.apply(patch);
                                autosurgeon::reconcile(tx, &doc).wrap_err("error reconciling")?;
                                let doc = Arc::new(doc);
                                let heads = crate::ChangeHashSet(tx.get_heads().into());
                                cache.insert(id.clone(), (doc, heads));
                            }
                        }
                        eyre::Ok(())
                    })
                })
                .map_err(|err| err.error)
        })
        .await
        .wrap_err(ERROR_TOKIO)??;
        eyre::Ok(())
    }

    /// Apply a batch of patches to documents. Each patch must include the target `id`.
    pub async fn update_batch(&self, docs: Vec<DocPatch>) -> Res<()> {
        use futures::StreamExt;
        let mut stream =
            futures::stream::iter(docs.into_iter().enumerate().map(|(ii, patch)| async move {
                self.update(patch)
                    .await
                    .wrap_err_with(|| format!("error on patch at index {ii}"))
            }))
            .buffer_unordered(16);
        let mut errors = vec![];
        while let Some(res) = stream.next().await {
            if let Err(err) = res {
                errors.push(err);
            }
        }
        if !errors.is_empty() {
            let mut root_err = ferr!("error applying patches");
            for err in errors {
                use color_eyre::Section;
                root_err = root_err.section(err);
            }
            Err(root_err)
        } else {
            Ok(())
        }
    }

    // Delete: evict from drawer and cache (document remains in repo for now)
    pub async fn del(&self, id: &DocId) -> Res<bool> {
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
        let client_acx = AmCtx::boot(
            utils_rs::am::Config {
                peer_id: "client".into(),
                storage: utils_rs::am::StorageConfig::Memory,
            },
            Some(samod::AlwaysAnnounce),
        )
        .await?;
        let server_acx = AmCtx::boot(
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
        let client_repo = DrawerRepo::load(client_acx.clone(), drawer_doc_id.clone()).await?;
        let server_repo = DrawerRepo::load(server_acx.clone(), drawer_doc_id.clone()).await?;

        let (server_notif_tx, mut server_notif_rx) = tokio::sync::mpsc::unbounded_channel();
        let _listener_handle = server_repo
            .register_listener(move |msg| server_notif_tx.send(msg).expect(ERROR_CHANNEL));

        let (client_notif_tx, mut client_notif_rx) = tokio::sync::mpsc::unbounded_channel();
        let _client_listener_handle = client_repo
            .register_listener(move |msg| client_notif_tx.send(msg).expect(ERROR_CHANNEL));

        let new_doc_id = client_repo
            .add(Doc {
                id: "client".into(),
                created_at: OffsetDateTime::now_utc(),
                updated_at: OffsetDateTime::now_utc(),
                content: crate::r#gen::doc::DocContent::Text("Hello, world!".into()),
                tags: vec![],
            })
            .await?;

        {
            let event =
                tokio::time::timeout(std::time::Duration::from_secs(1), client_notif_rx.recv())
                    .await
                    .wrap_err("timeout")?
                    .ok_or_eyre("channel closed")?;
            match &*event {
                DrawerEvent::DocAdded { id, heads: _ } => {
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
                DrawerEvent::DocAdded { id, heads: _ } => {
                    assert_eq!(*id, new_doc_id);
                }
                _ => eyre::bail!("unexpected event"),
            }
        }

        Ok(())
    }
}
