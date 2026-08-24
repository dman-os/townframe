use crate::interlude::*;

use crate::drawer::DrawerRepo;
use automerge::ActorId;
use big_repo::{
    BigDocHandle, BigRepoChangeFilter, BigRepoChangeListenerRegistration,
    BigRepoChangeNotification, BigRepoDocIdFilter,
};
use futures::future::BoxFuture;

#[async_trait]
pub trait AmStore: Hydrate + Reconcile + Send + Sync + 'static {
    fn prop() -> Cow<'static, str>;

    // async fn flush(&mut self, args: &mut Self::FlushArgs) -> Res<()> {
    async fn flush(
        &mut self,
        doc_handle: &BigDocHandle,
        actor_id: Option<ActorId>,
    ) -> Res<Option<automerge::ChangeHash>> {
        self.flush_with_prop(doc_handle, Self::prop(), actor_id)
            .await
    }

    async fn flush_with_prop(
        &mut self,
        doc_handle: &BigDocHandle,
        prop: Cow<'static, str>,
        actor_id: Option<ActorId>,
    ) -> Res<Option<automerge::ChangeHash>> {
        doc_handle
            .reconcile_prop_with_actor(automerge::ROOT, prop, self, actor_id)
            .await
    }

    async fn load(doc_handle: &BigDocHandle) -> Res<Self> {
        Self::load_from_prop(doc_handle, Self::prop()).await
    }

    async fn load_from_prop(doc_handle: &BigDocHandle, prop: Cow<'static, str>) -> Res<Self> {
        doc_handle
            .hydrate_path::<Self>(automerge::ROOT, vec![prop.into()])
            .await?
            .ok_or_eyre("unable to find obj in am")
            .map(|(val, _heads)| val)
    }

    async fn register_change_listener(
        big_repo: &SharedBigRepo,
        doc_id: &DocumentId,
        path: Vec<autosurgeon::Prop<'static>>,
    ) -> Res<(
        BigRepoChangeListenerRegistration,
        tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
    )> {
        Self::register_change_listener_for_prop(big_repo, doc_id, Self::prop(), path).await
    }

    async fn register_change_listener_for_prop(
        big_repo: &SharedBigRepo,
        doc_id: &DocumentId,
        prop: Cow<'static, str>,
        mut path: Vec<autosurgeon::Prop<'static>>,
    ) -> Res<(
        BigRepoChangeListenerRegistration,
        tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
    )> {
        path.insert(0, prop.into());
        big_repo
            .subscribe_change_listener(BigRepoChangeFilter {
                path,
                doc_id: Some(BigRepoDocIdFilter::new(*doc_id)),
                origin: None,
            })
            .await
    }
}

struct Inner<S> {
    store: S,
    doc_handle: BigDocHandle,
    store_prop: Option<String>,
    local_actor_id: ActorId,
    // flush_args: S::FlushArgs,
}

impl<S: AmStore> Inner<S> {
    async fn flush(&mut self) -> Res<Option<automerge::ChangeHash>> {
        let actor_id = self.local_actor_id.clone();
        match &self.store_prop {
            Some(prop) => {
                self.store
                    .flush_with_prop(&self.doc_handle, Cow::Owned(prop.clone()), Some(actor_id))
                    .await
            }
            None => self.store.flush(&self.doc_handle, Some(actor_id)).await,
        }
    }
}

pub struct AmStoreHandle<S: AmStore> {
    inner: Arc<tokio::sync::RwLock<Inner<S>>>,
}
impl<T: AmStore> Clone for AmStoreHandle<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S> AmStoreHandle<S>
where
    S: AmStore,
{
    pub fn new(
        store: S,
        //flush_args: S::FlushArgs,
        doc_handle: BigDocHandle,
        local_actor_id: ActorId,
    ) -> Self {
        Self::new_with_prop(store, doc_handle, None, local_actor_id)
    }

    pub fn new_with_prop(
        store: S,
        doc_handle: BigDocHandle,
        store_prop: Option<String>,
        local_actor_id: ActorId,
    ) -> Self {
        Self {
            inner: Arc::new(tokio::sync::RwLock::new(Inner {
                store,
                doc_handle,
                store_prop,
                local_actor_id,
            })),
        }
    }

    pub async fn query<F, O>(&self, fun: F) -> O
    where
        F: for<'a> FnOnce(&'a S) -> BoxFuture<'a, O>,
        O: Sized,
    {
        let guard = self.inner.read().await;
        fun(&guard.store).await
    }

    pub async fn query_sync<F, O>(&self, fun: F) -> O
    where
        F: FnOnce(&S) -> O,
        O: Sized,
    {
        let guard = self.inner.read().await;
        fun(&guard.store)
    }

    pub async fn mutate<F, O>(&self, fun: F) -> Res<(O, Option<automerge::ChangeHash>)>
    where
        O: Sized,
        F: for<'a> FnOnce(&'a mut S) -> BoxFuture<'a, O>,
    {
        let mut guard = self.inner.write().await;
        let res = fun(&mut guard.store).await;
        let hash = guard.flush().await?;
        Ok((res, hash))
    }

    pub async fn try_mutate<O, F>(&self, fun: F) -> Res<(O, Option<automerge::ChangeHash>)>
    where
        O: Sized,
        F: for<'a> FnOnce(&'a mut S) -> BoxFuture<'a, Res<O>>,
    {
        let mut guard = self.inner.write().await;
        let res = fun(&mut guard.store).await?;
        let hash = guard.flush().await?;
        Ok((res, hash))
    }

    pub async fn mutate_sync<F, O>(&self, fun: F) -> Res<(O, Option<automerge::ChangeHash>)>
    where
        F: FnOnce(&mut S) -> O,
        O: Sized,
    {
        let mut guard = self.inner.write().await;
        let res = fun(&mut guard.store);
        let hash = guard.flush().await?;
        Ok((res, hash))
    }

    pub async fn try_mutate_sync<F, O>(&self, fun: F) -> Res<(O, Option<automerge::ChangeHash>)>
    where
        F: FnOnce(&mut S) -> Res<O>,
        O: Sized,
    {
        let mut guard = self.inner.write().await;
        let res = fun(&mut guard.store)?;
        let hash = guard.flush().await?;
        Ok((res, hash))
    }
}

#[derive(Clone, Hydrate, Reconcile)]
pub struct Versioned<T> {
    pub vtag: VersionTag,
    // #[serde(flatten)]
    pub val: T,
}

#[derive(Clone, Debug)]
pub struct VersionTag {
    pub version: Uuid,
    pub actor_id: ActorId,
}

impl VersionTag {
    /// Create a version tag to be used when
    /// updating an instance.
    pub fn update(actor_id: ActorId) -> Self {
        Self {
            version: Uuid::new_v4(),
            actor_id,
        }
    }

    /// Create a version tag to be used for a new
    /// instance of a tagged version.
    pub fn mint(actor_id: ActorId) -> Self {
        Self {
            version: Uuid::nil(),
            actor_id,
        }
    }

    /// Create a static and empty version tag to be used
    /// in determinstic contexts like automerge version updates.
    pub(crate) fn nil() -> VersionTag {
        Self {
            version: Uuid::nil(),
            actor_id: [0u8; 16].into(),
        }
    }

    pub fn hydrate_bytes_or_warn(
        bytes: &[u8],
        patch_path: &impl std::fmt::Debug,
        key: &str,
        context: &str,
    ) -> Option<Self> {
        match <Self as Hydrate>::hydrate_bytes(bytes) {
            Ok(vtag) => Some(vtag),
            Err(err) => {
                warn!(
                    ?patch_path,
                    key = %key,
                    ?err,
                    context,
                    "ignoring malformed vtag patch"
                );
                None
            }
        }
    }
}

impl Reconcile for VersionTag {
    type Key<'a> = autosurgeon::reconcile::NoKey;

    fn reconcile<R: autosurgeon::Reconciler>(&self, mut reconciler: R) -> Result<(), R::Error> {
        let mut buf = [0_u8; 32];
        buf[0..16].copy_from_slice(self.version.as_bytes());
        buf[16..].copy_from_slice(self.actor_id.to_bytes());
        reconciler.bytes(buf)
    }
}

impl Hydrate for VersionTag {
    fn hydrate_bytes(bytes: &[u8]) -> Result<Self, autosurgeon::HydrateError> {
        if bytes.len() != 32 {
            return Err(autosurgeon::HydrateError::unexpected(
                "version tag in 32 length byte array",
                format!("version tags has byte length of {}", bytes.len()),
            ));
        }
        Ok(Self {
            version: {
                let mut buf = [0_u8; 16];
                buf.copy_from_slice(&bytes[0..16]);
                Uuid::from_bytes(buf)
            },
            actor_id: {
                let mut buf = [0_u8; 16];
                buf.copy_from_slice(&bytes[16..]);
                buf.into()
            },
        })
    }
}

impl<T> std::ops::Deref for Versioned<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.val
    }
}
impl<T> std::ops::DerefMut for Versioned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.val
    }
}

impl<T> Versioned<T> {
    pub fn mint(actor_id: ActorId, value: T) -> Self {
        Self {
            vtag: VersionTag::mint(actor_id),
            val: value,
        }
    }

    pub fn update(actor_id: ActorId, value: T) -> Self {
        Self {
            vtag: VersionTag::update(actor_id),
            val: value,
        }
    }

    pub fn replace(&mut self, actor_id: ActorId, value: T) -> T {
        self.vtag = VersionTag::update(actor_id);
        std::mem::replace(&mut self.val, value)
    }

    pub fn get_val(&self) -> &T {
        &self.val
    }
}

/// A change notification forwarded from the switch sink to the store's
/// notif loop (mirrors the AmStore change-listener pattern).
#[derive(Debug, Clone)]
pub(crate) enum FacetStoreNotif {
    DocChanged {
        doc_id: daybook_types::doc::DocId,
        heads: ChangeHashSet,
    },
}

/// A store that lives in a facet of a drawer doc — the drawer-doc counterpart
/// of `AmStore`. Serialization is serde_json (drawer facets are JSON values);
/// the in-memory projection is kept live by a notif loop fed by
/// `FacetStoreSink` (which forwards switch Doc events for the store's doc).
#[async_trait]
pub trait FacetStore:
    serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
    /// The facet key (tag + id) the store lives at.
    fn facet_key() -> daybook_types::doc::FacetKey;

    /// Seed used when the facet is absent.
    fn seed() -> Self;
}

/// One write version of a store's facet: the heads after the write, the
/// deserialized value, and the author of the write. The facet content and
/// its dmeta marker live in the same change, so each version is a
/// consistent snapshot; the author lets consumers filter out local writes
/// (applied synchronously by mutators).
#[derive(Debug, Clone)]
pub struct FacetStoreVersion<S> {
    pub heads: ChangeHashSet,
    pub value: S,
    pub actor_id: ActorId,
}
/// Handle to a `FacetStore`: the in-memory projection plus the notif loop
/// that keeps it live under remote updates. Writes go through the drawer at
/// the heads the projection was loaded at; unseen remote writes become
/// concurrent and merge by automerge rules (no CAS, no clobber).
pub struct FacetStoreHandle<S: FacetStore> {
    inner: Arc<tokio::sync::RwLock<FacetStoreInner<S>>>,
    doc_id: daybook_types::doc::DocId,
    notif_tx: tokio::sync::mpsc::UnboundedSender<FacetStoreNotif>,
    notif_handle: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    cancel_token: tokio_util::sync::CancellationToken,
}

impl<S: FacetStore> Clone for FacetStoreHandle<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            doc_id: self.doc_id.clone(),
            notif_tx: self.notif_tx.clone(),
            notif_handle: Arc::clone(&self.notif_handle),
            cancel_token: self.cancel_token.clone(),
        }
    }
}

impl<S: FacetStore> Drop for FacetStoreHandle<S> {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        if let Ok(mut guard) = self.notif_handle.try_lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
    }
}

struct FacetStoreInner<S> {
    store: S,
    drawer: Arc<DrawerRepo>,
    branch: daybook_types::doc::BranchPathBuf,
    /// The heads the in-memory projection was loaded/updated at — the base
    /// for the next write.
    loaded_heads: Option<ChangeHashSet>,
    /// The drawer's content actor for this doc — the author of every write
    /// this store makes (via its flush). Consumers use it to filter local
    /// writes out of version histories. Resolved lazily (the doc may not be
    /// registered in the drawer yet at load time).
    local_writer_actor: Option<ActorId>,
}

impl<S: FacetStore> FacetStoreHandle<S> {
    /// Load the store from the drawer doc's facet at the branch's current
    /// heads (absent facet -> `S::seed()`), and spawn the notif loop.
    pub async fn load(
        drawer: Arc<DrawerRepo>,
        doc_id: daybook_types::doc::DocId,
        branch: daybook_types::doc::BranchPathBuf,
    ) -> Res<Self> {
        let (store, loaded_heads) = Self::hydrate(&drawer, &doc_id, &branch).await?;
        let (notif_tx, notif_rx) = tokio::sync::mpsc::unbounded_channel();
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let handle = Self {
            inner: Arc::new(tokio::sync::RwLock::new(FacetStoreInner {
                store,
                drawer,
                branch,
                loaded_heads,
                local_writer_actor: None,
            })),
            doc_id,
            notif_tx,
            notif_handle: Arc::new(tokio::sync::Mutex::new(None)),
            cancel_token: cancel_token.clone(),
        };
        let notif_handle = tokio::spawn({
            let inner = Arc::clone(&handle.inner);
            let doc_id = handle.doc_id.clone();
            let cancel_token = cancel_token.child_token();
            async move {
                facet_store_notif_loop::<S>(inner, doc_id, notif_rx, cancel_token)
                    .await
                    .expect("error handling facet store notifs")
            }
        });
        *handle.notif_handle.lock().await = Some(notif_handle);
        Ok(handle)
    }

    /// The drawer doc id this store lives in (for the sink's doc-id check).
    pub fn doc_id(&self) -> &daybook_types::doc::DocId {
        &self.doc_id
    }

    /// The switch sink that keeps this store live: forwards Doc events for
    /// the store's doc to the notif loop (no reads in the sink).
    pub fn sink(&self) -> FacetStoreSink<S> {
        FacetStoreSink::new(self.clone())
    }

    /// The drawer's content actor for this doc — the author of every write
    /// this store makes. Resolved lazily (the doc may not be registered in
    /// the drawer at load time); None until resolvable.
    pub async fn local_writer_actor(&self) -> Option<ActorId> {
        if let Some(actor) = self.inner.read().await.local_writer_actor.clone() {
            return Some(actor);
        }
        let (drawer, branch) = {
            let guard = self.inner.read().await;
            (Arc::clone(&guard.drawer), guard.branch.clone())
        };
        let actor = drawer.resolve_content_actor(&self.doc_id, &branch).await;
        if let Some(actor) = &actor {
            self.inner.write().await.local_writer_actor = Some(actor.clone());
        }
        actor
    }

    /// The store's value at the given heads (None when the facet is absent
    /// at those heads).
    pub async fn at(&self, heads: &ChangeHashSet) -> Res<Option<S>> {
        let (drawer, branch, facet_key) = {
            let guard = self.inner.read().await;
            (
                Arc::clone(&guard.drawer),
                guard.branch.clone(),
                S::facet_key(),
            )
        };
        let Some(doc) = drawer
            .get_doc_with_facets_at_branch_heads(
                &self.doc_id,
                &branch,
                heads,
                Some(vec![facet_key.clone()]),
            )
            .await?
        else {
            return Ok(None);
        };
        let Some(raw) = doc.facets.get(&facet_key) else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_value(raw.clone())?))
    }

    /// All write versions of the facet between `from` and `to`, oldest
    /// first, each with its author. `from: None` enumerates from the
    /// doc's beginning.
    pub async fn versions(
        &self,
        from: Option<&ChangeHashSet>,
        to: &ChangeHashSet,
    ) -> Res<Vec<FacetStoreVersion<S>>> {
        let (drawer, branch, facet_key) = {
            let guard = self.inner.read().await;
            (
                Arc::clone(&guard.drawer),
                guard.branch.clone(),
                S::facet_key(),
            )
        };
        let from_heads: Vec<automerge::ChangeHash> = from
            .map(|heads| heads.as_ref().to_vec())
            .unwrap_or_default();
        let to_heads: Vec<automerge::ChangeHash> = to.as_ref().to_vec();
        let points = drawer
            .get_facet_write_points(&self.doc_id, &branch, &facet_key, &from_heads, &to_heads)
            .await?;
        let mut versions = Vec::with_capacity(points.len());
        for (heads, actor_id) in points {
            let Some(value) = self.at(&heads).await? else {
                continue;
            };
            versions.push(FacetStoreVersion {
                heads,
                value,
                actor_id,
            });
        }
        Ok(versions)
    }

    /// Re-hydrate the in-memory projection from the drawer at current heads.
    pub async fn reload(&self) -> Res<()> {
        Self::reload_inner(&self.inner, &self.doc_id).await
    }

    async fn reload_inner(
        inner: &Arc<tokio::sync::RwLock<FacetStoreInner<S>>>,
        doc_id: &daybook_types::doc::DocId,
    ) -> Res<()> {
        let (store, loaded_heads) = {
            let guard = inner.read().await;
            Self::hydrate(&guard.drawer, doc_id, &guard.branch).await?
        };
        let mut guard = inner.write().await;
        guard.store = store;
        guard.loaded_heads = loaded_heads;
        Ok(())
    }

    /// Stop the notif loop and join it.
    pub async fn stop(&self) -> Res<()> {
        self.cancel_token.cancel();
        if let Some(handle) = self.notif_handle.lock().await.take() {
            handle.await?;
        }
        Ok(())
    }

    async fn hydrate(
        drawer: &DrawerRepo,
        doc_id: &daybook_types::doc::DocId,
        branch: &daybook_types::doc::BranchPathBuf,
    ) -> Res<(S, Option<ChangeHashSet>)> {
        let heads = drawer
            .get_doc_branches(doc_id)
            .await?
            .and_then(|entry| entry.branches.get(branch.as_str()).cloned());
        let Some(doc) = drawer
            .get_doc_with_facets_at_branch(doc_id, branch, Some(vec![S::facet_key()]))
            .await?
        else {
            return Ok((S::seed(), heads));
        };
        let store = match doc.facets.get(&S::facet_key()) {
            Some(raw) => serde_json::from_value(raw.clone())?,
            None => S::seed(),
        };
        Ok((store, heads))
    }

    fn build_patch(&self, store: &S) -> Res<daybook_types::doc::DocPatch> {
        Ok(daybook_types::doc::DocPatch {
            id: self.doc_id.clone(),
            facets_set: [(S::facet_key(), serde_json::to_value(store)?)].into(),
            facets_remove: vec![],
            user_path: None,
        })
    }

    /// Write a patch through the drawer and advance the loaded heads. No
    /// store lock is held here: the drawer write runs facet validation,
    /// which re-enters the store (and the drawer notif feeds the switch,
    /// whose sinks read the store) — holding the write lock across it would
    /// self-deadlock.
    async fn flush_patch(&self, patch: daybook_types::doc::DocPatch) -> Res<ChangeHashSet> {
        let (drawer, branch, loaded_heads) = {
            let guard = self.inner.read().await;
            (
                Arc::clone(&guard.drawer),
                guard.branch.clone(),
                guard.loaded_heads.clone(),
            )
        };
        drawer.update_at_heads(patch, &branch, loaded_heads).await?;
        let heads = drawer
            .get_doc_branches(&self.doc_id)
            .await?
            .and_then(|entry| entry.branches.get(branch.as_str()).cloned())
            .ok_or_eyre("facet store doc missing branch after write")?;
        self.inner.write().await.loaded_heads = Some(heads.clone());
        Ok(heads)
    }

    pub async fn query<F, O>(&self, fun: F) -> O
    where
        F: for<'a> FnOnce(&'a S) -> BoxFuture<'a, O>,
        O: Sized,
    {
        let guard = self.inner.read().await;
        fun(&guard.store).await
    }

    pub async fn query_sync<F, O>(&self, fun: F) -> O
    where
        F: FnOnce(&S) -> O,
        O: Sized,
    {
        let guard = self.inner.read().await;
        fun(&guard.store)
    }

    pub async fn mutate<F, O>(&self, fun: F) -> Res<(O, ChangeHashSet)>
    where
        O: Sized,
        F: for<'a> FnOnce(&'a mut S) -> BoxFuture<'a, O>,
    {
        let (res, patch) = {
            let mut inner = self.inner.write().await;
            let res = fun(&mut inner.store).await;
            let patch = self.build_patch(&inner.store)?;
            (res, patch)
        };
        let heads = self.flush_patch(patch).await?;
        Ok((res, heads))
    }

    pub async fn try_mutate<O, F>(&self, fun: F) -> Res<(O, ChangeHashSet)>
    where
        O: Sized,
        F: for<'a> FnOnce(&'a mut S) -> BoxFuture<'a, Res<O>>,
    {
        let (res, patch) = {
            let mut inner = self.inner.write().await;
            let res = fun(&mut inner.store).await?;
            let patch = self.build_patch(&inner.store)?;
            (res, patch)
        };
        let heads = self.flush_patch(patch).await?;
        Ok((res, heads))
    }

    pub async fn mutate_sync<F, O>(&self, fun: F) -> Res<(O, ChangeHashSet)>
    where
        F: FnOnce(&mut S) -> O,
        O: Sized,
    {
        let (res, patch) = {
            let mut inner = self.inner.write().await;
            let res = fun(&mut inner.store);
            let patch = self.build_patch(&inner.store)?;
            (res, patch)
        };
        let heads = self.flush_patch(patch).await?;
        Ok((res, heads))
    }

    pub async fn try_mutate_sync<O, F>(&self, fun: F) -> Res<(O, ChangeHashSet)>
    where
        F: FnOnce(&mut S) -> Res<O>,
        O: Sized,
    {
        let (res, patch) = {
            let mut inner = self.inner.write().await;
            let res = fun(&mut inner.store)?;
            let patch = self.build_patch(&inner.store)?;
            (res, patch)
        };
        let heads = self.flush_patch(patch).await?;
        Ok((res, heads))
    }
}

/// The store's notif loop: reloads the in-memory projection on each
/// forwarded Doc change. Holds only the inner + doc id (not the handle), so
/// dropping the handle without `stop()` still aborts the loop.
async fn facet_store_notif_loop<S: FacetStore>(
    inner: Arc<tokio::sync::RwLock<FacetStoreInner<S>>>,
    _doc_id: daybook_types::doc::DocId,
    mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<FacetStoreNotif>,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Res<()> {
    loop {
        let notif = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => break,
            msg = notif_rx.recv() => match msg {
                Some(notif) => notif,
                None => break,
            },
        };
        let FacetStoreNotif::DocChanged { doc_id, heads } = notif;
        debug!(%doc_id, ?heads, "facet store notif: reloading");
        FacetStoreHandle::<S>::reload_inner(&inner, &doc_id).await?;
    }
    Ok(())
}

/// Switch sink that keeps a `FacetStore` live: forwards Doc events for the
/// store's doc to its notif loop. Cheap — no reads in the sink; the loop
/// does the reload.
pub struct FacetStoreSink<S: FacetStore> {
    handle: FacetStoreHandle<S>,
}

impl<S: FacetStore> FacetStoreSink<S> {
    pub fn new(handle: FacetStoreHandle<S>) -> Self {
        Self { handle }
    }
}

#[async_trait]
impl<S: FacetStore> crate::rt::switch::SwitchSink for FacetStoreSink<S> {
    fn interest(&self) -> crate::rt::switch::SwtchSinkInterest {
        crate::rt::switch::SwtchSinkInterest {
            consume_doc: true,
            consume_drawer: false,
            consume_plugs: false,
            consume_dispatch: false,
            consume_config: false,
            drawer_predicate: Some(daybook_types::manifest::DocPredicateClause::HasTag(
                S::facet_key().tag.to_string().into(),
            )),
        }
    }

    async fn on_event(
        &mut self,
        event: &crate::rt::switch::SwitchEvent,
        _ctx: &crate::rt::switch::SwitchSinkCtx<'_>,
    ) -> Res<crate::rt::switch::SwitchSinkOutcome> {
        let crate::rt::switch::SwitchEvent::Doc(evt) = event else {
            return Ok(crate::rt::switch::SwitchSinkOutcome::default());
        };
        if evt.doc_id == *self.handle.doc_id() {
            self.handle
                .notif_tx
                .send(FacetStoreNotif::DocChanged {
                    doc_id: evt.doc_id.clone(),
                    heads: evt.new_heads.clone(),
                })
                .map_err(|_| ferr!("facet store notif channel closed"))?;
        }
        Ok(crate::rt::switch::SwitchSinkOutcome::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hydrate_bytes_or_warn_skips_invalid_length() {
        let patch_path = vec!["root".to_string(), "vtag".to_string()];
        assert!(
            VersionTag::hydrate_bytes_or_warn(&[0_u8; 31], &patch_path, "vtag", "config").is_none()
        );
    }

    #[test]
    fn hydrate_bytes_or_warn_parses_valid_version_tag() {
        let patch_path = vec!["root".to_string(), "vtag".to_string()];
        let bytes = [0_u8; 32];
        let vtag =
            VersionTag::hydrate_bytes_or_warn(&bytes, &patch_path, "vtag", "config").unwrap();
        assert_eq!(vtag.version, Uuid::nil());
        assert_eq!(vtag.actor_id, [0_u8; 16].into());
    }
}
