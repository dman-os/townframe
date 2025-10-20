use samod::DocumentId;
use utils_rs::am::AmCtx;

use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use crate::gen::doc::{Doc, DocId, DocPatch};
use std::str::FromStr;

mod ffi;

#[derive(Default, Reconcile, Hydrate)]
pub struct DrawerStore {
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
        drawer_doc_id: DocumentId,
        on_change: F,
    ) -> Res<()>
    where
        F: Fn(Vec<utils_rs::am::changes::ChangeNotification>) + Send + Sync + 'static,
    {
        acx.change_manager()
            .add_listener(
                utils_rs::am::changes::ChangeFilter {
                    doc_id: Some(drawer_doc_id),
                    path: vec![Self::PROP.into()],
                },
                on_change,
            )
            .await;

        Ok(())
    }
}

impl crate::stores::Store for DrawerStore {
    type FlushArgs = (AmCtx, DocumentId);

    async fn flush(&mut self, (acx, app_doc_id): &mut Self::FlushArgs) -> Res<()> {
        acx.reconcile_prop(app_doc_id, automerge::ROOT, Self::PROP, self)
            .await
    }
}

struct DrawerRepo {
    // drawer_doc_id: DocumentId,
    acx: AmCtx,
    store: crate::stores::StoreHandle<DrawerStore>,
    // in-memory cache of document handles
    handles: Arc<DHashMap<DocId, samod::DocHandle>>,
    cache: Arc<DHashMap<DocId, Doc>>,
    registry: Arc<crate::repos::ListenersRegistry>,
}

// Minimal event enum so Kotlin can refresh via ffiList on changes
#[derive(Debug, Clone, uniffi::Enum)]
pub enum DrawerEvent {
    ListChanged,
}

impl DrawerRepo {
    async fn load(acx: AmCtx, drawer_doc_id: DocumentId) -> Res<Self> {
        let registry = crate::repos::ListenersRegistry::new();

        DrawerStore::register_change_listener(&acx, drawer_doc_id.clone(), {
            let registry = registry.clone();

            move |_notifications| {
                // Notify repo listeners that the docs list changed
                registry.notify(DrawerEvent::ListChanged);
            }
        })
        .await?;
        let store = DrawerStore::load(&acx, &drawer_doc_id).await?;
        let store = crate::stores::StoreHandle::new(store, (acx.clone(), drawer_doc_id));

        let repo = Self {
            acx,
            // drawer_doc_id,
            store,
            registry: registry.clone(),
            handles: default(),
            cache: default(),
        };

        Ok(repo)
    }

    // NOTE: old contains/insert/remove removed. Use add/get/update/del instead.

    async fn list(&self) -> Vec<DocId> {
        self.store
            .query_sync(|store| store.map.keys().cloned().collect())
            .await
    }

    // Create a new doc (Automerge), reconcile the provided `Doc` into it, store and cache handle,
    // and add its id to the drawer set.
    async fn add(&self, mut new_doc: Doc) -> Res<DocId> {
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

        // store id in drawer AM
        {
            let heads = utils_rs::am::serialize_commit_heads(&heads);
            self.store
                .mutate_sync(|store| store.map.insert(new_doc.id.clone(), heads))
                .await?;
        }

        // cache the handle under the doc's Uuid id
        let out_id = new_doc.id.clone();
        self.cache.insert(new_doc.id.clone(), new_doc);
        self.handles.insert(out_id.clone(), handle);
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
    async fn get(&self, id: DocId) -> Res<Option<Doc>> {
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
    async fn update_batch(&self, docs: Vec<DocPatch>) -> Res<()> {
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
    async fn del(&self, id: DocId) -> Res<bool> {
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

impl crate::repos::Repo for DrawerRepo {
    type Event = DrawerEvent;
    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }
}
