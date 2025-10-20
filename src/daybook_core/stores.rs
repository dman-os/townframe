use futures::future::BoxFuture;

use crate::interlude::*;

#[async_trait::async_trait]
pub trait Store {
    type FlushArgs;
    async fn flush(&mut self, args: &mut Self::FlushArgs) -> Res<()>;
}

struct Inner<S: Store> {
    store: S,
    flush_args: S::FlushArgs,
}

impl<S: Store> Inner<S> {
    async fn flush(&mut self) -> Res<()> {
        self.store.flush(&mut self.flush_args).await
    }
}

pub struct StoreHandle<S: Store> {
    inner: Arc<tokio::sync::RwLock<Inner<S>>>,
}

impl<S> StoreHandle<S>
where
    S: Store,
{
    pub fn new(store: S, flush_args: S::FlushArgs) -> Self {
        Self {
            inner: Arc::new(tokio::sync::RwLock::new(Inner { store, flush_args })),
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

    pub async fn mutate<F, O>(&self, fun: F) -> Res<O>
    where
        O: Sized,
        F: for<'a> FnOnce(&'a mut S) -> BoxFuture<'a, O>,
    {
        let mut guard = self.inner.write().await;
        let res = fun(&mut guard.store).await;
        guard.flush().await?;
        Ok(res)
    }

    pub async fn try_mutate<O, F>(&self, fun: F) -> Res<O>
    where
        O: Sized,
        F: for<'a> FnOnce(&'a mut S) -> BoxFuture<'a, Res<O>>,
    {
        let mut guard = self.inner.write().await;
        let res = fun(&mut guard.store).await?;
        guard.flush().await?;
        Ok(res)
    }

    pub async fn mutate_sync<F, O>(&self, fun: F) -> Res<O>
    where
        F: FnOnce(&mut S) -> O,
        O: Sized,
    {
        let mut guard = self.inner.write().await;
        let res = fun(&mut guard.store);
        guard.flush().await?;
        Ok(res)
    }

    pub async fn try_mutate_sync<F, O>(&self, fun: F) -> Res<O>
    where
        F: FnOnce(&mut S) -> Res<O>,
        O: Sized,
    {
        let mut guard = self.inner.write().await;
        let res = fun(&mut guard.store)?;
        guard.flush().await?;
        Ok(res)
    }
}
