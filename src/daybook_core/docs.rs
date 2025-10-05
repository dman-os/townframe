use crate::interlude::*;

use crate::ffi::{FfiError, SharedFfiCtx};
use daybook_types::doc as generated_doc;
// Local Doc type used inside daybook_core so we can derive autosurgeon/uniffi
// traits reliably within this crate and convert to/from generated types.
#[derive(Debug, Clone, uniffi::Record, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Doc {
    pub id: String,
    #[serde(with = "api_utils_rs::codecs::sane_iso8601")]
    pub created_at: OffsetDateTime,
    #[serde(with = "api_utils_rs::codecs::sane_iso8601")]
    pub updated_at: OffsetDateTime,
    pub content: generated_doc::DocKind,
    pub tags: Vec<generated_doc::DocTag>,
}

impl TryFrom<generated_doc::Doc> for Doc {
    type Error = utils_rs::prelude::serde_json::Error;
    fn try_from(g: generated_doc::Doc) -> Result<Self, Self::Error> {
        // round-trip via serde Value to convert between crates
        let v = utils_rs::prelude::serde_json::to_value(&g)?;
        let out = utils_rs::prelude::serde_json::from_value(v)?;
        Ok(out)
    }
}

impl TryFrom<Doc> for generated_doc::Doc {
    type Error = utils_rs::prelude::serde_json::Error;
    fn try_from(d: Doc) -> Result<Self, Self::Error> {
        let v = utils_rs::prelude::serde_json::to_value(&d)?;
        let out = utils_rs::prelude::serde_json::from_value(v)?;
        Ok(out)
    }
}

#[derive(Default)]
pub struct DocsAm {
    map: HashMap<Uuid, Doc>,
}
impl DocsAm {
    // const PATH: &[&str] = &["docs"];
    pub const PROP: &str = "docs";
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
        let am = DocsAm::default();
        let am = Arc::new(tokio::sync::RwLock::new(am));
        let registry = crate::repos::ListenersRegistry::new();

        let repo = Arc::new(Self {
            fcx: fcx.clone(),
            am,
            registry: registry.clone(),
        });

        Ok(repo)
    }

    async fn get(&self, id: Uuid) -> Res<Option<Doc>> {
        let am = self.am.read().await;
        Ok(am.map.get(&id).cloned())
    }

    async fn set(&self, id: Uuid, val: Doc) -> Res<Option<Doc>> {
        let mut am = self.am.clone().write_owned().await;
        let ret = am.map.insert(id, val);
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

    // FFI-friendly JSON wrappers: use String (JSON) over generated `Doc` type so
    // UniFFI doesn't require Lift/Lower impls for `daybook_types::doc::Doc`.
    #[tracing::instrument(err, skip(self))]
    async fn ffi_get_json(self: Arc<Self>, id: Uuid) -> Result<Option<String>, FfiError> {
        let this = self.clone();
        let out = self.fcx.do_on_rt(async move { this.get(id).await }).await?;
        match out {
            Some(doc) => Ok(Some(serde_json::to_string(&doc)?)),
            None => Ok(None),
        }
    }

    #[tracing::instrument(err, skip(self, doc_json))]
    async fn ffi_set_json(self: Arc<Self>, id: Uuid, doc_json: String) -> Result<Option<String>, FfiError> {
        let this = self.clone();
        let doc: Doc = serde_json::from_str(&doc_json)?;
        let out = self
            .fcx
            .do_on_rt(async move { this.set(id, doc).await })
            .await?;
        Ok(out.map(|d| serde_json::to_string(&d).unwrap_or_default()))
    }

    #[tracing::instrument(err, skip(self))]
    async fn ffi_list_json(self: Arc<Self>) -> Result<Vec<String>, FfiError> {
        let this = self.clone();
        let out = self.fcx.do_on_rt(async move { this.list().await }).await?;
        Ok(out.into_iter().map(|d| serde_json::to_string(&d).unwrap_or_default()).collect())
    }
}
