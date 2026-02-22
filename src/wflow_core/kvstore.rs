use crate::interlude::*;

pub mod log;
pub mod metastore;
pub mod snapstore;

/// A keyvalue interface that provides atomic operations.
///
/// Atomic operations are single, indivisible operations. When a fault causes an atomic operation to
/// fail, it will appear to the invoker of the atomic operation that the action either completed
/// successfully or did nothing at all.
#[async_trait]
pub trait KvStore {
    async fn get(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>>;
    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Res<Option<Arc<[u8]>>>;
    async fn del(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>>;

    /// Atomically increment the value associated with the key in the store by the given delta. It
    /// returns the new value.
    ///
    /// If the key does not exist in the store, it creates a new key-value pair with the value set
    /// to the given delta.
    ///
    /// If the value exists but cannot be parsed as an i64, it returns an error.
    async fn increment(&self, key: &[u8], delta: i64) -> Res<i64>;

    /// Construct a new CAS operation. Implementors can map the underlying functionality
    /// (transactions, versions, etc) as desired.
    async fn new_cas(&self, key: &[u8]) -> Res<CasGuard>;
}

/// A handle to a CAS (compare-and-swap) operation.
///
/// This is a type-erased guard that uses dynamic dispatch to work with any store implementation.
pub struct CasGuard {
    current_cb: Arc<dyn Fn() -> Option<Arc<[u8]>> + Send + Sync>,
    #[allow(clippy::type_complexity)]
    swap_cb: Arc<
        dyn Fn(Arc<[u8]>) -> futures::future::BoxFuture<'static, Res<Result<(), CasError>>>
            + Send
            + Sync,
    >,
}

impl std::fmt::Debug for CasGuard {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("CasGuard").finish_non_exhaustive()
    }
}

impl CasGuard {
    /// Create a new CAS guard with the given callbacks.
    pub fn new(
        current_cb: impl Fn() -> Option<Arc<[u8]>> + Send + Sync + 'static,
        swap_cb: impl Fn(Arc<[u8]>) -> futures::future::BoxFuture<'static, Res<Result<(), CasError>>>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self {
            current_cb: Arc::new(current_cb),
            swap_cb: Arc::new(swap_cb),
        }
    }

    /// Get the current value of the key (if it exists). This allows for avoiding reads if all
    /// that is needed to ensure the atomicity of the operation.
    pub fn current(&self) -> Option<Arc<[u8]>> {
        (self.current_cb)()
    }

    /// Perform the swap operation. This consumes the guard.
    pub async fn swap(self, value: Arc<[u8]>) -> Res<Result<(), CasError>> {
        (self.swap_cb)(value).await
    }
}

/// The error returned by a CAS operation
#[derive(Debug)]
pub enum CasError {
    /// A store error occurred when performing the operation
    StoreError(eyre::Report),
    /// The CAS operation failed because the value was too old. This returns a new CAS handle
    /// for easy retries. Implementors MUST return a CAS handle that has been updated to the
    /// latest version or transaction.
    CasFailed(CasGuard),
}

impl std::fmt::Display for CasError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CasError::StoreError(err) => write!(fmt, "store error: {err}"),
            CasError::CasFailed(_) => write!(fmt, "CAS failed, value was modified"),
        }
    }
}

impl std::error::Error for CasError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CasError::StoreError(err) => Some(err.as_ref()),
            CasError::CasFailed(_) => None,
        }
    }
}

impl From<eyre::Report> for CasError {
    fn from(err: eyre::Report) -> Self {
        CasError::StoreError(err)
    }
}

fn make_dhashmap_cas_guard(
    store: Arc<utils_rs::DHashMap<Arc<[u8]>, Arc<[u8]>>>,
    key: Arc<[u8]>,
    snapshot: Option<Arc<[u8]>>,
) -> CasGuard {
    let current_cb = {
        let snapshot = snapshot.clone();
        move || snapshot.clone()
    };
    let key_for_cb = Arc::clone(&key);
    let swap_cb =
        move |value: Arc<[u8]>| -> futures::future::BoxFuture<'static, Res<Result<(), CasError>>> {
            let store = Arc::clone(&store);
            let key = Arc::clone(&key_for_cb);
            let snapshot = snapshot.clone();
            Box::pin(async move {
                use dashmap::mapref::entry::Entry;
                match store.entry(key.clone()) {
                    Entry::Occupied(mut entry) => {
                        let current = Arc::clone(entry.get());
                        if snapshot.as_deref() == Some(current.as_ref()) {
                            entry.insert(value);
                            Ok(Ok(()))
                        } else {
                            let new_guard =
                                make_dhashmap_cas_guard(Arc::clone(&store), key, Some(current));
                            Ok(Err(CasError::CasFailed(new_guard)))
                        }
                    }
                    Entry::Vacant(entry) => {
                        if snapshot.is_none() {
                            entry.insert(value);
                            Ok(Ok(()))
                        } else {
                            let new_guard = make_dhashmap_cas_guard(Arc::clone(&store), key, None);
                            Ok(Err(CasError::CasFailed(new_guard)))
                        }
                    }
                }
            })
        };

    CasGuard::new(current_cb, swap_cb)
}

#[async_trait]
impl KvStore for Arc<utils_rs::DHashMap<Arc<[u8]>, Arc<[u8]>>> {
    async fn get(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        Ok(DHashMap::get(self, key).map(|val| Arc::clone(val.value())))
    }
    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Res<Option<Arc<[u8]>>> {
        Ok(self.insert(key, value))
    }
    async fn del(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        Ok(self.remove(key).map(|(_, val)| val))
    }
    async fn increment(&self, key: &[u8], delta: i64) -> Res<i64> {
        use dashmap::mapref::entry::Entry;

        let key: Arc<[u8]> = key.into();
        match self.entry(key) {
            Entry::Occupied(mut entry) => {
                let bytes = entry.get();
                if bytes.len() != 8 {
                    return Err(ferr!(
                        "cannot increment: value is not a valid i64 (expected 8 bytes, got {})",
                        bytes.len()
                    ));
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(bytes.as_ref());
                let current_value = i64::from_le_bytes(buf);
                let new_value = current_value
                    .checked_add(delta)
                    .ok_or_else(|| ferr!("integer overflow in increment"))?;
                entry.insert(new_value.to_le_bytes().into());
                Ok(new_value)
            }
            Entry::Vacant(entry) => {
                entry.insert(delta.to_le_bytes().into());
                Ok(delta)
            }
        }
    }

    async fn new_cas(&self, key: &[u8]) -> Res<CasGuard> {
        let snapshot = self.get(key).await?;
        Ok(make_dhashmap_cas_guard(
            Arc::clone(self),
            key.into(),
            snapshot,
        ))
    }
}

#[cfg(any(test, feature = "test-harness"))]
pub mod tests {
    use super::*;

    pub async fn test_kv_store_impl(store: Arc<dyn KvStore + Send + Sync>) -> Res<()> {
        let key1: Arc<[u8]> = b"key1".to_vec().into();
        let val1: Arc<[u8]> = b"value1".to_vec().into();
        let val2: Arc<[u8]> = b"value2".to_vec().into();

        // Test basic set/get
        store.set(Arc::clone(&key1), Arc::clone(&val1)).await?;
        assert_eq!(store.get(&key1).await?, Some(Arc::clone(&val1)));

        // Test overwrite
        store.set(Arc::clone(&key1), Arc::clone(&val2)).await?;
        assert_eq!(store.get(&key1).await?, Some(Arc::clone(&val2)));

        // Test del
        store.del(&key1).await?;
        assert_eq!(store.get(&key1).await?, None);

        // Test increment
        let counter_key: Arc<[u8]> = b"counter".to_vec().into();
        assert_eq!(store.increment(&counter_key, 5).await?, 5);
        assert_eq!(store.increment(&counter_key, 10).await?, 15);
        assert_eq!(store.increment(&counter_key, -3).await?, 12);

        // Test CAS
        let cas_key: Arc<[u8]> = b"cas_key".to_vec().into();
        let cas_val1: Arc<[u8]> = b"cas_val1".to_vec().into();
        let cas_val2: Arc<[u8]> = b"cas_val2".to_vec().into();
        let cas_val3: Arc<[u8]> = b"cas_val3".to_vec().into();

        // Initial CAS (from None)
        let cas = store.new_cas(&cas_key).await?;
        assert_eq!(cas.current(), None);
        cas.swap(Arc::clone(&cas_val1)).await??;
        assert_eq!(store.get(&cas_key).await?, Some(Arc::clone(&cas_val1)));

        // Successful swap
        let cas = store.new_cas(&cas_key).await?;
        assert_eq!(cas.current(), Some(Arc::clone(&cas_val1)));
        cas.swap(Arc::clone(&cas_val2)).await??;
        assert_eq!(store.get(&cas_key).await?, Some(Arc::clone(&cas_val2)));

        // Failed swap (concurrent modification)
        let cas_guard = store.new_cas(&cas_key).await?;
        // modify the value before the swap
        store
            .set(Arc::clone(&cas_key), Arc::clone(&cas_val3))
            .await?;

        match cas_guard.swap(Arc::clone(&cas_val1)).await? {
            Err(CasError::CasFailed(new_guard)) => {
                // Should return new guard with latest value
                assert_eq!(new_guard.current(), Some(Arc::clone(&cas_val3)));
            }
            _ => panic!("Expected CasFailed error"),
        }

        Ok(())
    }

    pub async fn test_kv_store_concurrency(store: Arc<dyn KvStore + Send + Sync>) -> Res<()> {
        let counter_key: Arc<[u8]> = b"concurrent_counter".to_vec().into();
        let num_tasks = 32;
        let increments_per_task = 500;

        let mut tasks = Vec::new();
        for _ in 0..num_tasks {
            let store = Arc::clone(&store);
            let key = Arc::clone(&counter_key);
            tasks.push(tokio::spawn(async move {
                for _ in 0..increments_per_task {
                    store.increment(&key, 1).await.expect("increment");
                    tokio::task::yield_now().await;
                }
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        let final_val = store.get(&counter_key).await?.unwrap();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&final_val);
        let final_count = i64::from_le_bytes(buf);

        assert_eq!(final_count, (num_tasks * increments_per_task) as i64);
        Ok(())
    }

    #[tokio::test]
    async fn test_dhashmap_kvstore() -> Res<()> {
        #[allow(clippy::type_complexity)]
        let store: Arc<DHashMap<Arc<[u8]>, Arc<[u8]>>> = Arc::new(DHashMap::default());
        let store_dyn: Arc<dyn KvStore + Send + Sync> = Arc::new(store);
        test_kv_store_impl(Arc::clone(&store_dyn)).await?;
        test_kv_store_concurrency(store_dyn).await
    }

    #[test]
    fn test_kv_store_concurrency_loom() {
        use futures::executor::block_on;
        use loom::thread;

        loom::model(|| {
            #[allow(clippy::type_complexity)]
            let store: Arc<DHashMap<Arc<[u8]>, Arc<[u8]>>> = Arc::new(DHashMap::default());
            let key: Arc<[u8]> = b"loom_counter".to_vec().into();

            let threads: Vec<_> = (0..4)
                .map(|_| {
                    let store = Arc::clone(&store);
                    let key = Arc::clone(&key);
                    thread::spawn(move || {
                        for _ in 0..3 {
                            block_on(async {
                                store.increment(&key, 1).await.expect("increment");
                            });
                            thread::yield_now();
                        }
                    })
                })
                .collect();

            for t in threads {
                let _ = t.join();
            }

            let final_val = block_on(store.get(&key)).unwrap().unwrap();
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&final_val);
            let final_count = i64::from_le_bytes(buf);
            assert_eq!(final_count, 12);
        });
    }
}
