use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use crate::gen::doc::DocId;

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
    am: Arc<tokio::sync::RwLock<DrawerAm>>,
    registry: Arc<crate::repos::ListenersRegistry>,
}

// Minimal event enum so Kotlin can refresh via ffiList on changes
#[derive(Debug, Clone, uniffi::Enum)]
pub enum DrawerEvent {
    ListChanged,
}

crate::repo_listeners!(DrawerRepo, DrawerEvent);

impl DrawerRepo {
    async fn load(fcx: SharedFfiCtx) -> Res<Arc<Self>> {
        let am = DrawerAm::load(&fcx.cx).await?;
        let am = Arc::new(tokio::sync::RwLock::new(am));
        let registry = crate::repos::ListenersRegistry::new();

        let repo = Arc::new(Self {
            fcx: fcx.clone(),
            am,
            registry: registry.clone(),
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

    async fn contains(&self, id: Uuid) -> Res<bool> {
        let am = self.am.read().await;
        Ok(am.map.contains_key(&id))
    }

    async fn insert(&self, id: Uuid) -> Res<bool> {
        let mut am = self.am.clone().write_owned().await;
        // return true when a new entry was added
        let was_present = am.map.insert(id, true).is_some();
        am.flush(&self.fcx.cx).await?;
        self.registry.notify(DrawerEvent::ListChanged);
        Ok(!was_present)
    }

    async fn remove(&self, id: Uuid) -> Res<bool> {
        let mut am = self.am.clone().write_owned().await;
        let existed = am.map.remove(&id).is_some();
        am.flush(&self.fcx.cx).await?;
        self.registry.notify(DrawerEvent::ListChanged);
        Ok(existed)
    }

    async fn list(&self) -> Res<Vec<DocId>> {
        let am = self.am.read().await;
        Ok(am.map.keys().cloned().collect())
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

    #[tracing::instrument(err, skip(self))]
    async fn ffi_contains(self: Arc<Self>, id: Uuid) -> Result<bool, FfiError> {
        let this = self.clone();
        Ok(self.fcx.do_on_rt(async move { this.contains(id).await }).await?)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_insert(self: Arc<Self>, id: Uuid) -> Result<bool, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.insert(id).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_remove(self: Arc<Self>, id: Uuid) -> Result<bool, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.remove(id).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_list(self: Arc<Self>) -> Result<Vec<DocId>, FfiError> {
        let this = self.clone();
        let out = self.fcx.do_on_rt(async move { this.list().await }).await?;
        Ok(out)
    }
}
