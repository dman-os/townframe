use automerge::ChangeHash;
use samod::DocumentId;
use utils_rs::am::{serialize_commit_heads, AmCtx};

use crate::interlude::*;

use crate::gen::doc::{Doc, DocId, DocPatch};
use crate::repos::Repo;
use std::str::FromStr;

#[derive(Default, Reconcile, Hydrate)]
pub struct DrawerStore {
    // FIXME: use changehash newtype that uses multihash
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    map: HashMap<DocId, Vec<String>>,
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

#[async_trait::async_trait]
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
    cache: Arc<DHashMap<DocId, Doc>>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
    broker: Arc<utils_rs::am::changes::DocChangeBroker>,
}

// Minimal event enum so Kotlin can refresh via ffiList on changes
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum DrawerEvent {
    ListChanged,
    DocAdded {
        id: DocId,
        heads: Vec<String>,
    },
    DocUpdated {
        id: DocId,
        new_heads: Vec<String>,
        old_heads: Vec<String>,
    },
    DocDeleted {
        id: DocId,
        old_heads: Vec<String>,
    },
}

pub enum DrawerUpdate {}

impl DrawerRepo {
    pub async fn load(acx: AmCtx, drawer_doc_id: DocumentId) -> Res<Self> {
        let registry = crate::repos::ListenersRegistry::new();

        let store = DrawerStore::load(&acx, &drawer_doc_id).await?;
        let store = crate::stores::StoreHandle::new(store, (acx.clone(), drawer_doc_id.clone()));

        let (notif_tx, mut notif_rx) = tokio::sync::mpsc::unbounded_channel::<
            Vec<utils_rs::am::changes::ChangeNotification>,
        >();

        let broker = {
            let handle = acx
                .find_doc(&drawer_doc_id)
                .await?
                .expect("doc should have been loaded");
            acx.change_manager().add_doc(handle)
        };

        DrawerStore::register_change_listener(&acx, &broker, vec!["map".into()], {
            move |notifs| notif_tx.send(notifs).expect("channel error")
        })
        .await?;

        let _notif_worker = tokio::spawn({
            let registry = registry.clone();
            let acx = acx.clone();
            let store = store.clone();
            async move {
                while let Some(notifs) = notif_rx.recv().await {
                    for notif in notifs {
                        // make sure the notif is on store.map
                        match &notif.patch.path[..] {
                            [(obj_id, automerge::Prop::Map(key))]
                                // FIXME: the first path of the obj_id is the patched obj, right??
                                if *obj_id == automerge::ROOT && key == "map" =>
                            {
                                match &notif.patch.action {
                                    automerge::PatchAction::PutMap {
                                        key: new_doc_id,
                                        value: (val, obj_id),
                                        ..
                                    } => {
                                        let Some(automerge::ObjType::List) = val.to_objtype()
                                        else {
                                            panic!("schema violation");
                                        };

                                        let new_heads = acx
                                            .hydrate_path_at_head::<Vec<String>>(
                                                &drawer_doc_id,
                                                &notif.heads,
                                                obj_id.clone(),
                                                vec![],
                                            )
                                            .await
                                            .expect("error hydrating at head")
                                            .expect("schema violation");

                                        let old_heads = store
                                            .mutate_sync({
                                                let key = new_doc_id.clone();
                                                |store| store.map.insert(key, new_heads.clone())
                                            })
                                            .await?;
                                        if let Some(old_heads) = old_heads {
                                            registry.notify(DrawerEvent::DocUpdated {
                                                id: new_doc_id.clone(),
                                                new_heads,
                                                old_heads,
                                            })
                                        } else {
                                            registry.notify(DrawerEvent::DocAdded {
                                                id: new_doc_id.clone(),
                                                heads: new_heads,
                                            })
                                        }
                                    }
                                    automerge::PatchAction::DeleteMap { key } => {
                                        let old_heads = store
                                                    .mutate_sync(|store| store.map.remove(key))
                                                    .await?;
                                        if let Some(old_heads) = old_heads {
                                            registry.notify(DrawerEvent::DocDeleted {
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
                            _ => {
                                info!(?notif.patch, "XXX weird patch");
                            }
                        }
                    }
                    // Notify repo listeners that the docs list changed
                    registry.notify(DrawerEvent::ListChanged);
                }
                eyre::Ok(())
            }
        });

        let repo = Self {
            acx,
            // drawer_doc_id,
            store,
            registry: registry.clone(),
            handles: default(),
            cache: default(),
            broker,
        };

        Ok(repo)
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
                                .map_err(|err| eyre::eyre!(err.to_string()))
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
        .wrap_err("tokio error")??;
        let heads: Arc<[ChangeHash]> = heads.into();

        let str_heads = serialize_commit_heads(&heads);

        // store id in drawer AM
        {
            self.store
                .mutate_sync(|store| store.map.insert(new_doc.id.clone(), str_heads.clone()))
                .await?;
        }

        // cache the handle under the doc's Uuid id
        let out_id = new_doc.id.clone();
        self.cache.insert(new_doc.id.clone(), new_doc);
        self.handles.insert(out_id.clone(), handle);
        self.registry.notify(DrawerEvent::DocAdded {
            id: out_id.clone(),
            heads: str_heads,
        });
        self.registry.notify(DrawerEvent::ListChanged);
        Ok(out_id)
    }

    async fn get_handle(&self, id: &DocId) -> Res<Option<samod::DocHandle>> {
        match self.handles.get(id) {
            Some(handle) => Ok(Some(handle.clone())),
            None => {
                if self
                    .store
                    .query_sync(|store| store.map.contains_key(id))
                    .await
                {
                    return Ok(None);
                }
                // Not in cache: check if the drawer actually lists this id
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
    pub async fn get(&self, id: DocId) -> Res<Option<Doc>> {
        if let Some(cached) = self.cache.get(&id) {
            return Ok(Some(cached.clone()));
        }
        let Some(handle) = self.get_handle(&id).await? else {
            return Ok(None);
        };
        let doc = tokio::task::block_in_place(move || {
            handle.with_document(move |doc| {
                let value: Doc = autosurgeon::hydrate(doc).wrap_err("error hydrating")?;
                eyre::Ok(value)
            })
        })?;
        self.cache.insert(id, doc.clone());

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
                .with_document(move |doc| {
                    doc.transact(move |tx| {
                        match cache.get_mut(&id) {
                            Some(mut val) => {
                                val.apply(patch);
                                autosurgeon::reconcile(tx, &*val).wrap_err("error reconciling")?;
                            }
                            None => {
                                let mut val: Doc =
                                    autosurgeon::hydrate(tx).wrap_err("error hydrating")?;
                                val.apply(patch);
                                autosurgeon::reconcile(tx, &val).wrap_err("error reconciling")?;
                                cache.insert(id.clone(), val);
                            }
                        }
                        eyre::Ok(())
                    })
                })
                .map_err(|err| err.error)
        })
        .await
        .wrap_err("tokio error")??;
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
    pub async fn del(&self, id: DocId) -> Res<bool> {
        let doc_key = id.clone();
        let existed = self
            .store
            .mutate_sync(|store| store.map.remove(&doc_key).is_some())
            .await?;
        self.cache.remove(&doc_key);
        self.handles.remove(&doc_key);
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
    async fn test_one() -> Res<()> {
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
            .register_listener(move |msg| server_notif_tx.send(msg).expect("channel error"));

        let (client_notif_tx, mut client_notif_rx) = tokio::sync::mpsc::unbounded_channel();
        let _client_listener_handle = client_repo
            .register_listener(move |msg| client_notif_tx.send(msg).expect("channel error"));

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
                tokio::time::timeout(std::time::Duration::from_secs(10), server_notif_rx.recv())
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
