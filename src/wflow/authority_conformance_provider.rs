//! Minimal WASI keyvalue host used by the native/Wasm authority conformance test.
//!
//! The fixture imports keyvalue even when a case does not use it. Keeping this
//! provider local to the test makes workload binding real without bringing the
//! old partition-worker test harness back into the crate.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::HostPlugin;
use wash_runtime::plugin::WitInterfaces;
use wash_runtime::wasmtime::component::HasSelf;
use wash_runtime::wasmtime::component::Resource;
use wash_runtime::wit::WitInterface;
use wash_runtime::wit::WitWorld;

const WASI_KEYVALUE_ID: &str = "wasi-keyvalue";

pub(crate) type BucketHandle = String;

mod bindings {
    wash_runtime::wasmtime::component::bindgen!({
        path: "../wash_plugin_wflow/wit",
        world: "keyvalue",
        imports: { default: async | trappable | tracing },
        with: {
            "wasi:keyvalue/store.bucket": crate::authority_conformance_provider::BucketHandle,
        },
    });
}

use bindings::wasi::keyvalue::store::Error as StoreError;
use bindings::wasi::keyvalue::store::KeyResponse;

#[derive(Clone, Default)]
pub(crate) struct WasiKeyvalue {
    storage: Arc<RwLock<HashMap<Arc<str>, HashMap<String, HashMap<String, Vec<u8>>>>>>,
}

impl WasiKeyvalue {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl bindings::wasi::keyvalue::store::Host for SharedWashCtx {
    async fn open(
        &mut self,
        identifier: String,
    ) -> wasmtime::Result<Result<Resource<BucketHandle>, StoreError>> {
        let plugin = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID);
        plugin
            .storage
            .write()
            .await
            .entry(Arc::clone(&self.active_ctx.workload_id))
            .or_default()
            .entry(identifier.clone())
            .or_default();
        Ok(Ok(self.table.push(identifier)?))
    }
}

impl bindings::wasi::keyvalue::store::HostBucket for SharedWashCtx {
    async fn get(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
    ) -> wasmtime::Result<Result<Option<Vec<u8>>, StoreError>> {
        let bucket_name = self.table.get(&bucket)?;
        let plugin = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID);
        let storage = plugin.storage.read().await;
        let value = storage
            .get(&self.active_ctx.workload_id)
            .and_then(|buckets| buckets.get(bucket_name))
            .and_then(|bucket| bucket.get(&key))
            .cloned();
        Ok(Ok(value))
    }

    async fn set(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
        value: Vec<u8>,
    ) -> wasmtime::Result<Result<(), StoreError>> {
        let bucket_name = self.table.get(&bucket)?;
        let plugin = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID);
        let mut storage = plugin.storage.write().await;
        let Some(bucket) = storage
            .entry(Arc::clone(&self.active_ctx.workload_id))
            .or_default()
            .get_mut(bucket_name)
        else {
            return Ok(Err(StoreError::Other(format!(
                "bucket '{bucket_name}' does not exist"
            ))));
        };
        bucket.insert(key, value);
        Ok(Ok(()))
    }

    async fn delete(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
    ) -> wasmtime::Result<Result<(), StoreError>> {
        let bucket_name = self.table.get(&bucket)?;
        let plugin = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID);
        let mut storage = plugin.storage.write().await;
        let Some(bucket) = storage
            .entry(Arc::clone(&self.active_ctx.workload_id))
            .or_default()
            .get_mut(bucket_name)
        else {
            return Ok(Err(StoreError::Other(format!(
                "bucket '{bucket_name}' does not exist"
            ))));
        };
        bucket.remove(&key);
        Ok(Ok(()))
    }

    async fn exists(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
    ) -> wasmtime::Result<Result<bool, StoreError>> {
        let bucket_name = self.table.get(&bucket)?;
        let plugin = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID);
        let storage = plugin.storage.read().await;
        let exists = storage
            .get(&self.active_ctx.workload_id)
            .and_then(|buckets| buckets.get(bucket_name))
            .is_some_and(|bucket| bucket.contains_key(&key));
        Ok(Ok(exists))
    }

    async fn list_keys(
        &mut self,
        bucket: Resource<BucketHandle>,
        cursor: Option<u64>,
    ) -> wasmtime::Result<Result<KeyResponse, StoreError>> {
        let bucket_name = self.table.get(&bucket)?;
        let plugin = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID);
        let storage = plugin.storage.read().await;
        let Some(bucket) = storage
            .get(&self.active_ctx.workload_id)
            .and_then(|buckets| buckets.get(bucket_name))
        else {
            return Ok(Err(StoreError::Other(format!(
                "bucket '{bucket_name}' does not exist"
            ))));
        };
        let mut keys: Vec<_> = bucket.keys().cloned().collect();
        keys.sort();
        let start = cursor.unwrap_or(0) as usize;
        let end = (start + 100).min(keys.len());
        Ok(Ok(KeyResponse {
            keys: keys[start..end].to_vec(),
            cursor: (end < keys.len()).then_some(end as u64),
        }))
    }

    async fn drop(&mut self, bucket: Resource<BucketHandle>) -> wasmtime::Result<()> {
        self.table.delete(bucket)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl HostPlugin for WasiKeyvalue {
    fn id(&self) -> &'static str {
        WASI_KEYVALUE_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: std::collections::HashSet::from([WitInterface::from(
                "wasi:keyvalue/store@0.2.0-draft",
            )]),
            ..Default::default()
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        if interfaces
            .iter()
            .any(|interface| interface.namespace == "wasi" && interface.package == "keyvalue")
        {
            bindings::wasi::keyvalue::store::add_to_linker::<_, HasSelf<SharedWashCtx>>(
                item.linker(),
                |ctx| ctx,
            )?;
        }
        Ok(())
    }
}
