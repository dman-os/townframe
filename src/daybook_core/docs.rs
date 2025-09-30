use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

#[derive(Debug, Clone, Reconcile, Hydrate, uniffi::Record)]
pub struct Doc {
    #[key]
    pub id: Uuid,
    #[autosurgeon(with = "crate::am::autosurgeon_date")]
    pub timestamp: OffsetDateTime,
}

#[derive(Reconcile, Hydrate, Default)]
pub struct DocsAm {
    #[autosurgeon(with = "autosurgeon::map_with_parseable_keys")]
    map: HashMap<Uuid, Doc>,
}
impl DocsAm {
    // const PATH: &[&str] = &["docs"];
    pub const PROP: &str = "docs";

    async fn load(cx: &Ctx) -> Res<Self> {
        cx.acx
            .hydrate_path::<Self>(automerge::ROOT, vec![Self::PROP.into()])
            .await?
            .ok_or_eyre("unable to find obj in am")
    }

    async fn flush(&self, cx: &Ctx) -> Res<()> {
        cx.acx
            .reconcile_prop(automerge::ROOT, Self::PROP, self)
            .await
    }

    /// Register a change listener for docs changes
    async fn register_change_listener<F>(cx: &Ctx, on_change: F) -> Res<()>
    where
        F: Fn(Vec<crate::am::changes::ChangeNotification>) + Send + Sync + 'static,
    {
        cx.acx
            .change_manager()
            .register_change_listener(vec![Self::PROP.into()], on_change)
            .await;
        Ok(())
    }
}

#[derive(uniffi::Object)]
struct DocsRepo {
    fcx: SharedFfiCtx,
    am: Arc<tokio::sync::RwLock<DocsAm>>,
    registry: Arc<crate::repos::ListenersRegistry>,
}

// Minimal event enum so Kotlin can refresh via ffiList on changes
#[derive(Debug, Clone, uniffi::Enum)]
pub enum DocsEvent {
    ListChanged,
}

crate::repo_listeners!(DocsRepo, DocsEvent);

impl DocsRepo {
    async fn load(fcx: SharedFfiCtx) -> Res<Arc<Self>> {
        let am = DocsAm::load(&fcx.cx).await?;
        let am = Arc::new(tokio::sync::RwLock::new(am));
        let registry = crate::repos::ListenersRegistry::new();

        let repo = Arc::new(Self {
            fcx: fcx.clone(),
            am,
            registry: registry.clone(),
        });

        // Register change listener to automatically notify repo listeners
        DocsAm::register_change_listener(&fcx.cx, {
            let registry = registry.clone();
            move |_notifications| {
                // Notify repo listeners that the docs list changed
                registry.notify(DocsEvent::ListChanged);
            }
        })
        .await?;

        Ok(repo)
    }

    async fn get(&self, id: Uuid) -> Res<Option<Doc>> {
        let am = self.am.read().await;
        Ok(am.map.get(&id).cloned())
    }

    async fn set(&self, id: Uuid, val: Doc) -> Res<Option<Doc>> {
        let mut am = self.am.clone().write_owned().await;
        let ret = am.map.insert(id, val);
        am.flush(&self.fcx.cx).await?;
        // Notify listeners that the list changed
        self.registry.notify(DocsEvent::ListChanged);
        Ok(ret)
    }

    async fn list(&self) -> Res<Vec<Doc>> {
        let am = self.am.read().await;
        Ok(am.map.values().cloned().collect())
    }
}

#[uniffi::export]
impl DocsRepo {
    #[uniffi::constructor]
    #[tracing::instrument(err, skip(fcx))]
    async fn for_ffi(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let cx = fcx.clone();
        let this = fcx.do_on_rt(Self::load(cx)).await?;
        Ok(this)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_get(self: Arc<Self>, id: Uuid) -> Result<Option<Doc>, FfiError> {
        let this = self.clone();
        let out = self.fcx.do_on_rt(async move { this.get(id).await }).await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self, doc))]
    async fn ffi_set(self: Arc<Self>, id: Uuid, doc: Doc) -> Result<Option<Doc>, FfiError> {
        let this = self.clone();
        let out = self
            .fcx
            .do_on_rt(async move { this.set(id, doc).await })
            .await?;
        Ok(out)
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_list(self: Arc<Self>) -> Result<Vec<Doc>, FfiError> {
        let this = self.clone();
        let out = self.fcx.do_on_rt(async move { this.list().await }).await?;
        Ok(out)
    }
}
