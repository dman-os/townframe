use crate::interlude::*;

use futures::future::BoxFuture;

#[async_trait]
pub trait Store: Hydrate + Reconcile + Send + Sync + 'static {
    // type FlushArgs;
    const PROP: &'static str;

    // async fn flush(&mut self, args: &mut Self::FlushArgs) -> Res<()> {
    async fn flush(&mut self, acx: &mut AmCtx, doc_id: &DocumentId) -> Res<Option<automerge::ChangeHash>> {
        acx.reconcile_prop(doc_id, automerge::ROOT, Self::PROP, self)
            .await
    }

    async fn load(acx: &AmCtx, app_doc_id: &DocumentId) -> Res<Self> {
        acx.hydrate_path::<Self>(app_doc_id, automerge::ROOT, vec![Self::PROP.into()])
            .await?
            .ok_or_eyre("unable to find obj in am")
            .map(|(val, _heads)| val)
    }

    async fn register_change_listener<F>(
        acx: &AmCtx,
        broker: &utils_rs::am::changes::DocChangeBroker,
        mut path: Vec<autosurgeon::Prop<'static>>,
        on_change: F,
    ) -> Res<utils_rs::am::changes::ChangeListenerRegistration>
    where
        F: Fn(Vec<utils_rs::am::changes::ChangeNotification>) + Send + Sync + 'static,
    {
        path.insert(0, Self::PROP.into());
        let ticket = acx.change_manager()
            .add_listener(
                utils_rs::am::changes::ChangeFilter {
                    path,
                    doc_id: Some(broker.filter()),
                },
                Box::new(on_change),
            )
            .await;
        Ok(ticket)
    }
}

struct Inner<S> {
    store: S,
    acx: AmCtx,
    doc_id: DocumentId,
    // flush_args: S::FlushArgs,
}

impl<S: Store> Inner<S> {
    async fn flush(&mut self) -> Res<Option<automerge::ChangeHash>> {
        self.store.flush(&mut self.acx, &self.doc_id).await
    }
}

pub struct StoreHandle<S: Store> {
    inner: Arc<tokio::sync::RwLock<Inner<S>>>,
}
impl<T: Store> Clone for StoreHandle<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S> StoreHandle<S>
where
    S: Store,
{
    pub fn new(
        store: S,
        //flush_args: S::FlushArgs,
        acx: AmCtx,
        doc_id: DocumentId,
    ) -> Self {
        Self {
            inner: Arc::new(tokio::sync::RwLock::new(Inner { store, acx, doc_id })),
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
