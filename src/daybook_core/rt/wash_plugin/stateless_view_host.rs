use crate::interlude::*;

use wash_runtime::engine::ctx::SharedCtx as SharedWashCtx;
use wash_runtime::plugin::HostPlugin;
use wash_runtime::wit::{WitInterface, WitWorld};

use wash_plugin_wflow::{service_host, service_metastore, service_partition_host};

#[derive(Default)]
pub struct StatelessViewPlugin;

impl StatelessViewPlugin {
    pub const ID: &str = "townframe:stateless-view";

    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl HostPlugin for StatelessViewPlugin {
    fn id(&self) -> &'static str {
        Self::ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: std::collections::HashSet::new(),
            imports: std::collections::HashSet::from([WitInterface::from(
                "townframe:wflow/host,partition-host,metadata-store",
            )]),
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        _workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        _interface_configs: std::collections::HashSet<WitInterface>,
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
            if iface.namespace == "townframe" && iface.package == "wflow" {
                if iface.interfaces.contains("host") {
                    service_host::add_to_linker::<_, wasmtime::component::HasSelf<SharedWashCtx>>(
                        item.linker(),
                        |ctx| ctx,
                    )?;
                }
                if iface.interfaces.contains("partition-host") {
                    service_partition_host::add_to_linker::<
                        _,
                        wasmtime::component::HasSelf<SharedWashCtx>,
                    >(item.linker(), |ctx| ctx)?;
                }
                if iface.interfaces.contains("metadata-store") {
                    service_metastore::add_to_linker::<
                        _,
                        wasmtime::component::HasSelf<SharedWashCtx>,
                    >(item.linker(), |ctx| ctx)?;
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
