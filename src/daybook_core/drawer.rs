use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use crate::gen::doc::{Doc, DocId, DocPatch};
use std::str::FromStr;

#[derive(Default, Reconcile, Hydrate)]
pub struct DrawerAm {
    // FIXME: replace with hashset
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    map: HashMap<DocId, bool>,
}

impl DrawerAm {
    // const PATH: &[&str] = &["docs"];
    pub const PROP: &str = "docs";

    async fn load(cx: &Ctx) -> Res<Self> {
        cx.acx
            .hydrate_path::<Self>(
                cx.doc_drawer().clone(),
                automerge::ROOT,
                vec![Self::PROP.into()],
            )
            .await?
            .ok_or_eyre("unable to find obj in am")
    }

    async fn flush(&self, cx: &Ctx) -> Res<()> {
        cx.acx
            .reconcile_prop(cx.doc_drawer().clone(), automerge::ROOT, Self::PROP, self)
            .await
    }

    async fn register_change_listener<F>(cx: &Ctx, on_change: F) -> Res<()>
    where
        F: Fn(Vec<utils_rs::am::changes::ChangeNotification>) + Send + Sync + 'static,
    {
        cx.acx
            .change_manager()
            .add_listener(
                utils_rs::am::changes::ChangeFilter {
                    doc_id: Some(cx.doc_drawer().document_id().clone()),
                    path: vec![Self::PROP.into()],
                },
                on_change,
            )
            .await;

        Ok(())
    }
}

#[derive(uniffi::Object)]
struct DrawerRepo {
    fcx: SharedFfiCtx,
    drawer: Arc<tokio::sync::RwLock<DrawerAm>>,
    registry: Arc<crate::repos::ListenersRegistry>,
    // in-memory cache of document handles
    handles: Arc<DHashMap<DocId, samod::DocHandle>>,
    cache: Arc<DHashMap<DocId, Doc>>,
}

// Minimal event enum so Kotlin can refresh via ffiList on changes
#[derive(Debug, Clone, uniffi::Enum)]
pub enum DrawerEvent {
    ListChanged,
}

crate::repo_listeners!(DrawerRepo, DrawerEvent);

impl DrawerRepo {
    async fn load(fcx: SharedFfiCtx) -> Res<Arc<Self>> {
        let drawer = DrawerAm::load(&fcx.cx).await?;
        let drawer = Arc::new(tokio::sync::RwLock::new(drawer));
        let registry = crate::repos::ListenersRegistry::new();

        let repo = Arc::new(Self {
            fcx: fcx.clone(),
            drawer,
            registry: registry.clone(),
            handles: default(),
            cache: default(),
        });

        DrawerAm::register_change_listener(&fcx.cx, {
            let registry = registry.clone();

            move |_notifications| {
                // Notify repo listeners that the docs list changed
                registry.notify(DrawerEvent::ListChanged);
            }
        })
        .await?;

        Ok(repo)
    }

    // NOTE: old contains/insert/remove removed. Use add/get/update/del instead.

    async fn list(&self) -> Res<Vec<DocId>> {
        let am = self.drawer.read().await;
        Ok(am.map.keys().cloned().collect())
    }

    // Create a new doc (Automerge), reconcile the provided `Doc` into it, store and cache handle,
    // and add its id to the drawer set.
    async fn add(&self, mut new_doc: Doc) -> Res<DocId> {
        // Use AutoCommit for reconciliation
        let handle = self.fcx.cx.acx.add_doc(automerge::Automerge::new()).await?;

        new_doc.id = handle.document_id().to_string();

        let new_doc = tokio::task::spawn_blocking({
            let handle = handle.clone();
            move || {
                handle.with_document(move |doc_am| {
                    doc_am
                        .transact(move |tx| {
                            use automerge::transaction::Transactable;
                            tx.put(automerge::ROOT, "$schema", "daybook.doc")?;
                            autosurgeon::reconcile(tx, &new_doc)
                                .map_err(|err| eyre::eyre!(err.to_string()))
                                .wrap_err("error reconciling new doc")?;
                            eyre::Ok(new_doc)
                        })
                        .map(|val| val.result)
                        .map_err(|err| err.error)
                })
            }
        })
        .await
        .wrap_err("tokio error")??;

        // store id in drawer AM
        {
            let mut drawer = self.drawer.write().await;
            drawer.map.insert(new_doc.id.clone(), true);
            drawer.flush(&self.fcx.cx).await?;
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
                let am = self.drawer.read().await;
                if !am.map.contains_key(id) {
                    return Ok(None);
                }
                // Not in cache: check if the drawer actually lists this id
                let doc_id = samod::DocumentId::from_str(&id).wrap_err("invalid id")?;
                let Some(handle) = self.fcx.cx.acx.find_doc(doc_id).await? else {
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
        let mut am = self.drawer.write().await;
        let existed = am.map.remove(&doc_key).is_some();
        am.flush(&self.fcx.cx).await?;
        self.cache.remove(&doc_key);
        self.handles.remove(&doc_key);
        if existed {
            self.registry.notify(DrawerEvent::ListChanged);
        }
        Ok(existed)
    }
}

#[uniffi::export]
impl DrawerRepo {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx))]
    async fn for_ffi(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let cx = fcx.clone();
        let this = fcx
            .do_on_rt(Self::load(cx))
            .await
            .inspect_err(|err| tracing::error!(?err))?;
        Ok(this)
    }

    // old FFI wrappers for contains/insert/remove removed; use `ffi_get`, `ffi_add`, `ffi_update`, `ffi_del` instead
    #[tracing::instrument(err, skip(self))]
    async fn ffi_list(self: Arc<Self>) -> Result<Vec<DocId>, FfiError> {
        let this = self.clone();
        let out = self.fcx.do_on_rt(async move { this.list().await }).await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_get(self: Arc<Self>, id: DocId) -> Result<Option<Doc>, FfiError> {
        let this = self.clone();
        Ok(self.fcx.do_on_rt(async move { this.get(id).await }).await?)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_add(self: Arc<Self>, doc: Doc) -> Result<DocId, FfiError> {
        let this = self.clone();
        Ok(self
            .fcx
            .do_on_rt(async move { this.add(doc).await })
            .await?)
    }

    // singular update removed; expose batch-only API

    #[tracing::instrument(err, skip(self))]
    async fn ffi_update_batch(self: Arc<Self>, docs: Vec<DocPatch>) -> Result<(), FfiError> {
        let this = self.clone();
        Ok(self
            .fcx
            .do_on_rt(async move { this.update_batch(docs).await })
            .await?)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_del(self: Arc<Self>, id: DocId) -> Result<bool, FfiError> {
        let this = self.clone();
        Ok(self.fcx.do_on_rt(async move { this.del(id).await }).await?)
    }
}
