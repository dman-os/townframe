//! Host plugin implementation for townframe:pglite interfaces

use crate::interlude::*;

use std::collections::HashSet;

use wash_runtime::engine::ctx::Ctx as WashCtx;
use wash_runtime::wit::{WitInterface, WitWorld};

use crate::PgliteHandle;

mod binds_guest {
    wash_runtime::wasmtime::component::bindgen!({
        world: "guest",
        path: "wit",
        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
        additional_derives: [serde::Serialize, serde::Deserialize],
    });
}

pub use binds_guest::townframe::pglite::{query, types};

/// Host plugin providing pglite interfaces
pub struct PglitePlugin {
    handle: Arc<PgliteHandle>,
}

impl PglitePlugin {
    pub const ID: &'static str = "townframe:pglite";

    pub fn new(handle: Arc<PgliteHandle>) -> Self {
        Self { handle }
    }

    fn from_ctx(wcx: &WashCtx) -> Arc<Self> {
        let Some(this) = wcx.get_plugin::<Self>(Self::ID) else {
            panic!("pglite plugin not on ctx");
        };
        this
    }
}

#[async_trait]
impl wash_runtime::plugin::HostPlugin for PglitePlugin {
    fn id(&self) -> &'static str {
        Self::ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: HashSet::new(),
            imports: HashSet::from([WitInterface::from("townframe:pglite/query")]),
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        _workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        _interface_configs: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut wash_runtime::engine::workload::WorkloadItem<'a>,
        _interfaces: std::collections::HashSet<wash_runtime::wit::WitInterface>,
    ) -> anyhow::Result<()> {
        let world = item.world();
        for iface in world.imports {
            if iface.namespace == "townframe"
                && iface.package == "pglite"
                && iface.interfaces.contains("query")
            {
                query::add_to_linker::<_, wasmtime::component::HasSelf<WashCtx>>(
                    component.linker(),
                    |ctx| ctx,
                )?;
            }
        }
        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        _resolved: &wash_runtime::engine::workload::ResolvedWorkload,
        _component_id: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        _workload_id: &str,
        _interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        let _ = self.handle.shutdown().await;
        Ok(())
    }
}

impl query::Host for WashCtx {
    async fn query(
        &mut self,
        query_str: String,
        params: Vec<types::PgValue>,
    ) -> wasmtime::Result<Result<Vec<types::ResultRow>, types::QueryError>> {
        let plugin = PglitePlugin::from_ctx(self);
        match plugin
            .handle
            .query(&query_str, &params)
            .await
            .map_err(|e| types::QueryError::Unexpected(e.to_string()))
        {
            Ok(rows) => Ok(Ok(rows)),
            Err(err) => Ok(Err(err)),
        }
    }

    async fn query_batch(
        &mut self,
        query_str: String,
    ) -> wasmtime::Result<Result<(), types::QueryError>> {
        let plugin = PglitePlugin::from_ctx(self);
        match plugin
            .handle
            .query_batch(&query_str)
            .await
            .map_err(|e| types::QueryError::Unexpected(e.to_string()))
        {
            Ok(_) => Ok(Ok(())),
            Err(err) => Ok(Err(err)),
        }
    }
}
