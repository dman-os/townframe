//! Minimal WASI keyvalue host used by the native/Wasm authority conformance test.
//!
//! The fixture imports keyvalue even when a case does not use it. Keeping this
//! provider local to the test makes workload binding real without bringing the
//! old partition-worker test harness back into the crate.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use std::sync::RwLock;
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
        inline: r#"
            package townframe:authority-conformance;
            world keyvalue {
                import wasi:keyvalue/store@0.2.0-draft;
            }
        "#,
        world: "keyvalue",
        imports: { default: async | trappable | tracing },
        with: {
            "wasi:keyvalue/store.bucket": crate::authority_conformance_provider::BucketHandle,
        },
    });
}

use bindings::wasi::keyvalue::store::Error as StoreError;
use bindings::wasi::keyvalue::store::KeyResponse;

type Storage = HashMap<Arc<str>, HashMap<String, BTreeMap<String, Vec<u8>>>>;

#[derive(Clone, Default)]
pub(crate) struct WasiKeyvalue {
    // These in-memory critical sections contain no awaits or I/O.
    storage: Arc<RwLock<Storage>>,
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
            .expect("keyvalue state poisoned")
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
        let storage = plugin.storage.read().expect("keyvalue state poisoned");
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
        let mut storage = plugin.storage.write().expect("keyvalue state poisoned");
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
        let mut storage = plugin.storage.write().expect("keyvalue state poisoned");
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
        let storage = plugin.storage.read().expect("keyvalue state poisoned");
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
        let storage = plugin.storage.read().expect("keyvalue state poisoned");
        let Some(bucket) = storage
            .get(&self.active_ctx.workload_id)
            .and_then(|buckets| buckets.get(bucket_name))
        else {
            return Ok(Err(StoreError::Other(format!(
                "bucket '{bucket_name}' does not exist"
            ))));
        };
        let start = usize::try_from(cursor.unwrap_or(0))
            .unwrap_or(usize::MAX)
            .min(bucket.len());
        let keys: Vec<_> = bucket.keys().skip(start).take(100).cloned().collect();
        let end = start + keys.len();
        Ok(Ok(KeyResponse {
            keys,
            cursor: (end < bucket.len())
                .then(|| u64::try_from(end).expect("bucket length fits WASI cursor")),
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
