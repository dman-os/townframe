mod interlude {
    pub use std::sync::Arc;
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

use wash_runtime::engine::ctx::{Ctx as WashCtx, SharedCtx as SharedWashCtx};
use wash_runtime::wit::{WitInterface, WitWorld};

mod binds_guest {
    wash_runtime::wasmtime::component::bindgen!({
        world: "guest",

        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
    });
}

use binds_guest::townframe::utils::types;

mod binds_api_utils {
    wash_runtime::wasmtime::component::bindgen!({
        world: "imports",
        path: "../api_utils_rs/wit",

        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
    });
}

use binds_api_utils::townframe::api_utils::utils as api_utils_utils;

pub struct UtilsPlugin {}

pub struct Config {}

impl UtilsPlugin {
    pub fn new(_config: Config) -> Res<Arc<Self>> {
        Ok(Arc::new(Self {}))
    }

    const ID: &str = "townframe:utils";

    fn _from_ctx(wcx: &WashCtx) -> Arc<Self> {
        let Some(this) = wcx.get_plugin::<Self>(Self::ID) else {
            panic!("plugin not on ctx");
        };
        this
    }
}

#[async_trait]
impl wash_runtime::plugin::HostPlugin for UtilsPlugin {
    fn id(&self) -> &'static str {
        Self::ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: std::collections::HashSet::new(),
            imports: std::collections::HashSet::from([
                WitInterface::from("townframe:utils/types"),
                WitInterface::from("townframe:api-utils/utils"),
            ]),
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        _workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        interface_configs: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        // Validate that we can handle the requested interfaces
        for iface in &interface_configs {
            if iface.namespace == "townframe"
                && iface.package == "utils"
                && !iface.interfaces.contains("types")
                && !iface.interfaces.contains("utils")
            {
                anyhow::bail!("unsupported utils interface: {iface:?}");
            }
        }
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut wash_runtime::engine::workload::WorkloadItem<'a>,
        _interfaces: std::collections::HashSet<wash_runtime::wit::WitInterface>,
    ) -> anyhow::Result<()> {
        let world = item.world();
        for iface in world.imports {
            if iface.namespace == "townframe" && iface.package == "utils" {
                if iface.interfaces.contains("types") {
                    types::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        item.linker(),
                        |ctx| ctx,
                    )?;
                }
                if iface.interfaces.contains("utils") {
                    api_utils_utils::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        item.linker(),
                        |ctx| ctx,
                    )?;
                }
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
        _interfaces: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl types::Host for SharedWashCtx {
    async fn noop(
        &mut self,
        _inc: (
            types::ErrorsValidation,
            types::ErrorInternal,
            types::Datetime,
            types::Uuid,
        ),
    ) -> wasmtime::Result<Result<(), ()>> {
        // Implementation for the noop function from the types interface
        // This is a copy of the utils interface, so we can just return Ok
        Ok(Ok(()))
    }
}

impl api_utils_utils::Host for SharedWashCtx {
    async fn noop(
        &mut self,
        _inc: (
            api_utils_utils::ErrorsValidation,
            api_utils_utils::ErrorInternal,
            api_utils_utils::Datetime,
            api_utils_utils::Uuid,
        ),
    ) -> wasmtime::Result<Result<(), ()>> {
        Ok(Ok(()))
    }
}
