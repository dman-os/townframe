//! coped from wasmcloud/wash: Apache 2.0 license
//! # WASI KeyValue Memory Plugin
//!
//! This module implements an in-memory keyvalue plugin for the wasmCloud runtime,
//! providing the `wasi:keyvalue@0.2.0-draft` interfaces for development and testing scenarios.

use crate::interlude::*;
use std::collections::{HashMap, HashSet};

const WASI_KEYVALUE_ID: &str = "wasi-keyvalue";
use tokio::sync::RwLock;
use wasmtime::component::{HasSelf, Resource};

use wash_runtime::engine::ctx::{Ctx as WashCtx, SharedCtx as SharedWashCtx};
use wash_runtime::{
    engine::workload::WorkloadComponent,
    plugin::HostPlugin,
    wit::{WitInterface, WitWorld},
};

mod bindings {
    wasmtime::component::bindgen!({
        path: "../wash_plugin_wflow/wit",
        world: "keyvalue",
        imports: { default: async | trappable | tracing },
        with: {
            "wasi:keyvalue/store/bucket": crate::test::keyvalue_plugin::BucketHandle,
        },
    });
}

use bindings::wasi::keyvalue::store::{Error as StoreError, KeyResponse};

/// In-memory bucket representation
#[derive(Clone, Debug)]
pub struct BucketData {
    pub name: String,
    pub data: HashMap<String, Vec<u8>>,
    pub created_at: u64,
}

/// Resource representation for a bucket (key-value store)
pub type BucketHandle = String;

/// Memory-based keyvalue plugin
#[derive(Clone, Default)]
pub struct WasiKeyvalue {
    /// Storage for all buckets, keyed by workload ID, then bucket name
    #[allow(clippy::type_complexity)]
    storage: Arc<RwLock<HashMap<Arc<str>, HashMap<String, BucketData>>>>,
}

impl WasiKeyvalue {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new instance that shares the same storage
    pub fn with_shared_storage(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
        }
    }

    fn get_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Set a value in the keyvalue store directly (for testing)
    pub async fn set_value(
        &self,
        workload_id: &str,
        bucket: &str,
        key: &str,
        value: Vec<u8>,
    ) -> anyhow::Result<()> {
        let mut storage = self.storage.write().await;
        let workload_storage = storage.entry(workload_id.into()).or_default();

        // Create bucket if it doesn't exist
        if !workload_storage.contains_key(bucket) {
            let bucket_data = BucketData {
                name: bucket.to_string(),
                data: HashMap::new(),
                created_at: Self::get_timestamp(),
            };
            workload_storage.insert(bucket.to_string(), bucket_data);
        }

        match workload_storage.get_mut(bucket) {
            Some(bucket_data) => {
                bucket_data.data.insert(key.to_string(), value);
                Ok(())
            }
            None => Err(anyhow::anyhow!("bucket '{}' does not exist", bucket)),
        }
    }
}

// Implementation for the store interface
impl bindings::wasi::keyvalue::store::Host for SharedWashCtx {
    async fn open(
        &mut self,
        identifier: String,
    ) -> anyhow::Result<Result<Resource<BucketHandle>, StoreError>> {
        let Some(plugin) = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "keyvalue plugin not available".to_string(),
            )));
        };

        let mut storage = plugin.storage.write().await;
        let workload_storage = storage
            .entry(Arc::clone(&self.active_ctx.workload_id))
            .or_default();

        // Create bucket if it doesn't exist
        if !workload_storage.contains_key(&identifier) {
            let bucket_data = BucketData {
                name: identifier.clone(),
                data: HashMap::new(),
                created_at: WasiKeyvalue::get_timestamp(),
            };
            workload_storage.insert(identifier.clone(), bucket_data);
        }

        let resource = self.table.push(identifier)?;
        Ok(Ok(resource))
    }
}

// Resource host trait implementations for bucket
impl bindings::wasi::keyvalue::store::HostBucket for SharedWashCtx {
    async fn get(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
    ) -> anyhow::Result<Result<Option<Vec<u8>>, StoreError>> {
        let bucket_name = self.table.get(&bucket)?;

        let Some(plugin) = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "keyvalue plugin not available".to_string(),
            )));
        };

        let storage = plugin.storage.read().await;
        let empty_map = HashMap::new();
        let workload_storage = storage
            .get(&self.active_ctx.workload_id)
            .unwrap_or(&empty_map);

        match workload_storage.get(bucket_name) {
            Some(bucket_data) => {
                let value = bucket_data.data.get(&key).cloned();
                Ok(Ok(value))
            }
            None => Ok(Err(StoreError::Other(format!(
                "bucket '{bucket_name}' does not exist"
            )))),
        }
    }

    async fn set(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
        value: Vec<u8>,
    ) -> anyhow::Result<Result<(), StoreError>> {
        let bucket_name = self.table.get(&bucket)?;

        let Some(plugin) = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "keyvalue plugin not available".to_string(),
            )));
        };

        let mut storage = plugin.storage.write().await;
        let workload_storage = storage
            .entry(Arc::clone(&self.active_ctx.workload_id))
            .or_default();

        match workload_storage.get_mut(bucket_name) {
            Some(bucket_data) => {
                bucket_data.data.insert(key, value);
                Ok(Ok(()))
            }
            None => Ok(Err(StoreError::Other(format!(
                "bucket '{bucket_name}' does not exist"
            )))),
        }
    }

    async fn delete(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
    ) -> anyhow::Result<Result<(), StoreError>> {
        let bucket_name = self.table.get(&bucket)?;

        let Some(plugin) = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "keyvalue plugin not available".to_string(),
            )));
        };

        let mut storage = plugin.storage.write().await;
        let workload_storage = storage
            .entry(Arc::clone(&self.active_ctx.workload_id))
            .or_default();

        match workload_storage.get_mut(bucket_name) {
            Some(bucket_data) => {
                bucket_data.data.remove(&key);
                Ok(Ok(()))
            }
            None => Ok(Err(StoreError::Other(format!(
                "bucket '{bucket_name}' does not exist"
            )))),
        }
    }

    async fn exists(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
    ) -> anyhow::Result<Result<bool, StoreError>> {
        let bucket_name = self.table.get(&bucket)?;

        let Some(plugin) = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "keyvalue plugin not available".to_string(),
            )));
        };

        let storage = plugin.storage.read().await;
        let empty_map = HashMap::new();
        let workload_storage = storage.get(&self.active_ctx.id[..]).unwrap_or(&empty_map);

        match workload_storage.get(bucket_name) {
            Some(bucket_data) => Ok(Ok(bucket_data.data.contains_key(&key))),
            None => Ok(Err(StoreError::Other(format!(
                "bucket '{bucket_name}' does not exist"
            )))),
        }
    }

    async fn list_keys(
        &mut self,
        bucket: Resource<BucketHandle>,
        cursor: Option<u64>,
    ) -> anyhow::Result<Result<KeyResponse, StoreError>> {
        let bucket_name = self.table.get(&bucket)?;

        let Some(plugin) = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "keyvalue plugin not available".to_string(),
            )));
        };

        let storage = plugin.storage.read().await;
        let empty_map = HashMap::new();
        let workload_storage = storage.get(&self.active_ctx.id[..]).unwrap_or(&empty_map);

        match workload_storage.get(bucket_name) {
            Some(bucket_data) => {
                let mut keys: Vec<String> = bucket_data.data.keys().cloned().collect();
                keys.sort(); // Ensure consistent ordering

                // Simple cursor-based pagination - cursor is the index from previous page
                let start_index = cursor.unwrap_or(0) as usize;

                // Return up to 100 keys per page
                const PAGE_SIZE: usize = 100;
                let end_index = std::cmp::min(start_index + PAGE_SIZE, keys.len());
                let page_keys = keys[start_index..end_index].to_vec();

                // Set next cursor if there are more keys
                let next_cursor = if end_index < keys.len() {
                    Some(end_index as u64)
                } else {
                    None
                };

                Ok(Ok(KeyResponse {
                    keys: page_keys,
                    cursor: next_cursor,
                }))
            }
            None => Ok(Err(StoreError::Other(format!(
                "bucket '{bucket_name}' does not exist"
            )))),
        }
    }

    async fn drop(&mut self, rep: Resource<BucketHandle>) -> anyhow::Result<()> {
        tracing::debug!(
            workload_id = self.active_ctx.id,
            resource_id = ?rep,
            "Dropping bucket resource"
        );
        self.table.delete(rep)?;
        Ok(())
    }
}

// Implementation for the atomics interface
impl bindings::wasi::keyvalue::atomics::Host for SharedWashCtx {
    async fn increment(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
        delta: u64,
    ) -> anyhow::Result<Result<u64, StoreError>> {
        let bucket_name = self.table.get(&bucket)?;

        let Some(plugin) = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "keyvalue plugin not available".to_string(),
            )));
        };

        let mut storage = plugin.storage.write().await;
        let workload_storage = storage
            .entry(Arc::clone(&self.active_ctx.workload_id))
            .or_default();

        match workload_storage.get_mut(bucket_name) {
            Some(bucket_data) => {
                // Get current value, treating missing key as 0
                let current_bytes = bucket_data.data.get(&key);
                let current_value = if let Some(bytes) = current_bytes {
                    // Try to parse as u64 from 8-byte array
                    if bytes.len() == 8 {
                        u64::from_le_bytes(bytes.clone().try_into().unwrap_or([0; 8]))
                    } else {
                        // Try to parse as string representation
                        String::from_utf8_lossy(bytes).parse::<u64>().unwrap_or(0)
                    }
                } else {
                    0
                };

                let new_value = current_value.saturating_add(delta);

                // Store as 8-byte little-endian representation
                bucket_data
                    .data
                    .insert(key, new_value.to_le_bytes().to_vec());

                Ok(Ok(new_value))
            }
            None => Ok(Err(StoreError::Other(format!(
                "bucket '{bucket_name}' does not exist"
            )))),
        }
    }
}

// Implementation for the batch interface
impl bindings::wasi::keyvalue::batch::Host for SharedWashCtx {
    async fn get_many(
        &mut self,
        bucket: Resource<BucketHandle>,
        keys: Vec<String>,
    ) -> anyhow::Result<Result<Vec<Option<(String, Vec<u8>)>>, StoreError>> {
        let bucket_name = self.table.get(&bucket)?;

        let Some(plugin) = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "keyvalue plugin not available".to_string(),
            )));
        };

        let storage = plugin.storage.read().await;
        let empty_map = HashMap::new();
        let workload_storage = storage
            .get(&self.active_ctx.workload_id[..])
            .unwrap_or(&empty_map);

        match workload_storage.get(bucket_name) {
            Some(bucket_data) => {
                let results: Vec<Option<(String, Vec<u8>)>> = keys
                    .into_iter()
                    .map(|key| {
                        bucket_data
                            .data
                            .get(&key)
                            .cloned()
                            .map(|value| (key, value))
                    })
                    .collect();
                Ok(Ok(results))
            }
            None => Ok(Err(StoreError::Other(format!(
                "bucket '{bucket_name}' does not exist"
            )))),
        }
    }

    async fn set_many(
        &mut self,
        bucket: Resource<BucketHandle>,
        key_values: Vec<(String, Vec<u8>)>,
    ) -> anyhow::Result<Result<(), StoreError>> {
        let bucket_name = self.table.get(&bucket)?;

        let Some(plugin) = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "keyvalue plugin not available".to_string(),
            )));
        };

        let mut storage = plugin.storage.write().await;
        let workload_storage = storage
            .entry(Arc::clone(&self.active_ctx.workload_id))
            .or_default();

        match workload_storage.get_mut(bucket_name) {
            Some(bucket_data) => {
                for (key, value) in key_values {
                    bucket_data.data.insert(key, value);
                }
                Ok(Ok(()))
            }
            None => Ok(Err(StoreError::Other(format!(
                "bucket '{bucket_name}' does not exist"
            )))),
        }
    }

    async fn delete_many(
        &mut self,
        bucket: Resource<BucketHandle>,
        keys: Vec<String>,
    ) -> anyhow::Result<Result<(), StoreError>> {
        let bucket_name = self.table.get(&bucket)?;

        let Some(plugin) = self.active_ctx.get_plugin::<WasiKeyvalue>(WASI_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "keyvalue plugin not available".to_string(),
            )));
        };

        let mut storage = plugin.storage.write().await;
        let workload_storage = storage
            .entry(Arc::clone(&self.active_ctx.workload_id))
            .or_default();

        match workload_storage.get_mut(bucket_name) {
            Some(bucket_data) => {
                for key in keys {
                    bucket_data.data.remove(&key);
                }
                Ok(Ok(()))
            }
            None => Ok(Err(StoreError::Other(format!(
                "bucket '{bucket_name}' does not exist"
            )))),
        }
    }
}

#[async_trait::async_trait]
impl HostPlugin for WasiKeyvalue {
    fn id(&self) -> &'static str {
        WASI_KEYVALUE_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([WitInterface::from(
                "wasi:keyvalue/store,atomics,batch@0.2.0-draft",
            )]),
            ..Default::default()
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut wash_runtime::engine::workload::WorkloadItem<'a>,
        interfaces: std::collections::HashSet<wash_runtime::wit::WitInterface>,
    ) -> anyhow::Result<()> {
        // Check if any of the interfaces are wasi:keyvalue related
        let has_keyvalue = interfaces
            .iter()
            .any(|interface| interface.namespace == "wasi" && interface.package == "keyvalue");

        if !has_keyvalue {
            tracing::warn!(
                "WasiKeyvalue plugin requested for non-wasi:keyvalue interface(s): {:?}",
                interfaces
            );
            return Ok(());
        }

        tracing::debug!(
            workload_id = item.id(),
            "Adding keyvalue interfaces to linker for workload"
        );
        let linker = item.linker();

        bindings::wasi::keyvalue::store::add_to_linker::<_, HasSelf<SharedWashCtx>>(
            linker,
            |ctx| ctx,
        )?;
        bindings::wasi::keyvalue::atomics::add_to_linker::<_, HasSelf<SharedWashCtx>>(
            linker,
            |ctx| ctx,
        )?;
        bindings::wasi::keyvalue::batch::add_to_linker::<_, HasSelf<SharedWashCtx>>(
            linker,
            |ctx| ctx,
        )?;

        let id: Arc<str> = item.workload_id().into();
        tracing::debug!(
            workload_id = %id,
            "Successfully added keyvalue interfaces to linker for workload"
        );

        // Initialize storage for this workload
        let mut storage = self.storage.write().await;
        storage.entry(Arc::clone(&id)).or_insert_with(HashMap::new);

        tracing::debug!(%id,"WasiKeyvalue plugin bound to workload");

        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: std::collections::HashSet<wash_runtime::wit::WitInterface>,
    ) -> anyhow::Result<()> {
        // Clean up storage for this workload
        let mut storage = self.storage.write().await;
        storage.remove(workload_id);

        tracing::debug!("WasiKeyvalue plugin unbound from workload '{workload_id}'");

        Ok(())
    }
}
