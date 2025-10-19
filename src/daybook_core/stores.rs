use crate::interlude::*;

pub trait Store {
    type FlushArgs;
    async fn flush(&self, args: &Self::FlushArgs) -> Res<()>;
}
pub struct StoreHandle<S: Store> {
    store: Arc<tokio::sync::RwLock<S>>,
    flush_args: S::FlushArgs,
}

impl<S> StoreHandle<S>
where
    S: Store,
{
    pub fn new(store: S, flush_args: S::FlushArgs) -> Self {
        Self {
            store: Arc::new(tokio::sync::RwLock::new(store)),
            flush_args,
        }
    }

    pub async fn query<F, O, Fut>(&self, fun: F) -> O
    where
        F: FnOnce(&S) -> Fut,
        Fut: std::future::Future<Output = O> + Sized,
        O: Sized,
    {
        let guard = self.store.read().await;
        fun(&*guard).await
    }

    pub async fn query_sync<F, O>(&self, fun: F) -> O
    where
        F: FnOnce(&S) -> O,
        O: Sized,
    {
        let guard = self.store.read().await;
        fun(&*guard)
    }

    pub async fn mutate<F, O, Fut>(&mut self, fun: F) -> Res<O>
    where
        F: FnOnce(&mut S) -> Fut,
        Fut: std::future::Future<Output = O> + Sized,
        O: Sized,
    {
        let mut guard = self.store.write().await;
        let res = fun(&mut *guard).await;
        guard.flush(&self.flush_args).await?;
        Ok(res)
    }

    pub async fn try_mutate<F, O, Fut>(&mut self, fun: F) -> Res<O>
    where
        F: FnOnce(&mut S) -> Fut,
        Fut: std::future::Future<Output = Res<O>> + Sized,
        O: Sized,
    {
        let mut guard = self.store.write().await;
        let res = fun(&mut *guard).await?;
        guard.flush(&self.flush_args).await?;
        Ok(res)
    }

    pub async fn mutate_sync<F, O>(&mut self, fun: F) -> Res<O>
    where
        F: FnOnce(&mut S) -> O,
        O: Sized,
    {
        let mut guard = self.store.write().await;
        let res = fun(&mut *guard);
        guard.flush(&self.flush_args).await?;
        Ok(res)
    }

    pub async fn try_mutate_sync<F, O>(&mut self, fun: F) -> Res<O>
    where
        F: FnOnce(&mut S) -> Res<O>,
        O: Sized,
    {
        let mut guard = self.store.write().await;
        let res = fun(&mut *guard)?;
        guard.flush(&self.flush_args).await?;
        Ok(res)
    }
}

