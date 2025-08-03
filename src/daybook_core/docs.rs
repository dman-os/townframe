use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};

use std::collections::HashMap;

#[derive(Debug, Clone, autosurgeon::Reconcile, autosurgeon::Hydrate, uniffi::Record)]
pub struct Doc {
    #[key]
    pub id: Uuid,
    #[autosurgeon(with = "crate::am::autosurgeon_date")]
    pub timestamp: OffsetDateTime,
}

#[derive(autosurgeon::Reconcile, autosurgeon::Hydrate, Default)]
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
}

#[derive(uniffi::Object)]
struct DocsRepo {
    fcx: SharedFfiCtx,
    am: Arc<tokio::sync::RwLock<DocsAm>>,
}
impl DocsRepo {
    async fn load(fcx: SharedFfiCtx) -> Res<Arc<Self>> {
        let am = DocsAm::load(&fcx.cx).await?;
        let am = Arc::new(tokio::sync::RwLock::new(am));
        Ok(Arc::new(Self { fcx, am }))
    }

    async fn get(&self, id: Uuid) -> Res<Option<Doc>> {
        let am = self.am.read().await;
        Ok(am.map.get(&id).cloned())
    }

    async fn set(&self, id: Uuid, val: Doc) -> Res<Option<Doc>> {
        let mut am = self.am.clone().write_owned().await;
        let ret = am.map.insert(id, val);
        am.flush(&self.fcx.cx).await?;
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
    async fn for_ffi(fcx: SharedFfiCtx) -> Result<Arc<Self>, FfiError> {
        let cx = fcx.clone();
        let this = fcx.do_on_rt(Self::load(cx)).await?;
        Ok(this)
    }

    async fn ffi_get(self: Arc<Self>, id: Uuid) -> Result<Option<Doc>, FfiError> {
        let out = self
            .fcx
            .clone()
            .do_on_rt(async move { self.get(id).await })
            .await?;
        Ok(out)
    }

    async fn ffi_set(self: Arc<Self>, id: Uuid, doc: Doc) -> Result<Option<Doc>, FfiError> {
        let out = self
            .fcx
            .clone()
            .do_on_rt(async move { self.set(id, doc).await })
            .await?;
        Ok(out)
    }

    async fn ffi_list(self: Arc<Self>) -> Result<Vec<Doc>, FfiError> {
        let out = self
            .fcx
            .clone()
            .do_on_rt(async move { self.list().await })
            .await?;
        Ok(out)
    }
}
