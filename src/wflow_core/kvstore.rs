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
    swap_cb: Arc<
        dyn Fn(Arc<[u8]>) -> futures::future::BoxFuture<'static, Res<Result<(), CasError>>>
            + Send
            + Sync,
    >,
}

impl std::fmt::Debug for CasGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CasGuard").finish_non_exhaustive()
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CasError::StoreError(err) => write!(f, "store error: {err}"),
            CasError::CasFailed(_) => write!(f, "CAS failed, value was modified"),
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

#[async_trait]
impl KvStore for Arc<utils_rs::DHashMap<Arc<[u8]>, Arc<[u8]>>> {
    async fn get(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        Ok(DHashMap::get(self, key).map(|v| v.value().clone()))
    }
    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Res<Option<Arc<[u8]>>> {
        Ok(self.insert(key, value))
    }
    async fn del(&self, key: &[u8]) -> Res<Option<Arc<[u8]>>> {
        Ok(self.remove(key).map(|(_, val)| val))
    }
    async fn increment(&self, key: &[u8], delta: i64) -> Res<i64> {
        // Use CAS to atomically increment
        const MAX_CAS_RETRIES: usize = 100;
        let mut cas = self.new_cas(key).await?;
        for _attempt in 0..MAX_CAS_RETRIES {
            let current = cas.current();
            let current_value = if let Some(bytes) = current {
                // Try to parse as i64 (little-endian, 8 bytes)
                if bytes.len() == 8 {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&bytes);
                    i64::from_le_bytes(buf)
                } else {
                    return Err(ferr!(
                        "cannot increment: value is not a valid i64 (expected 8 bytes, got {})",
                        bytes.len()
                    ));
                }
            } else {
                0
            };

            let new_value = current_value
                .checked_add(delta)
                .ok_or_else(|| ferr!("integer overflow in increment"))?;

            // Store new value as little-endian bytes
            let new_bytes: Arc<[u8]> = new_value.to_le_bytes().into();
            match cas.swap(new_bytes).await? {
                Ok(()) => return Ok(new_value),
                Err(CasError::CasFailed(new_guard)) => {
                    cas = new_guard;
                    // Retry with new guard
                }
                Err(CasError::StoreError(err)) => return Err(err),
            }
        }
        Err(ferr!(
            "failed to increment after {MAX_CAS_RETRIES} CAS retries: concurrent modifications",
        ))
    }

    async fn new_cas(&self, key: &[u8]) -> Res<CasGuard> {
        // Take a snapshot of the current value
        let snapshot = self.get(key).await?;
        let key: Arc<[u8]> = key.into();
        let store = self.clone();

        let current_cb = {
            let snapshot = snapshot.clone();
            move || snapshot.clone()
        };

        let swap_cb = move |value: Arc<[u8]>| -> futures::future::BoxFuture<'static, Res<Result<(), CasError>>> {
            let store = store.clone();
            let key = key.clone();
            let snapshot = snapshot.clone();

            Box::pin(async move {
                // Get current value
                let current = store.get(&key).await?;
                // Compare with snapshot
                if current.as_ref().map(|v| v.as_ref()) == snapshot.as_ref().map(|v| v.as_ref()) {
                    // Values match, perform swap
                    store.set(key, value).await?;
                    Ok(Ok(()))
                } else {
                    // Values don't match, create new guard with updated snapshot
                    let new_guard = store.new_cas(&key).await?;
                    Ok(Err(CasError::CasFailed(new_guard)))
                }
            })
        };

        Ok(CasGuard::new(current_cb, swap_cb))
    }
}
