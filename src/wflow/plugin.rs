use crate::interlude::*;

use std::collections::HashSet;

use wash_runtime::wit::{WitInterface, WitWorld};

pub mod bindings_partition_host {
    wash_runtime::wasmtime::component::bindgen!({
        world: "rt-partition-host",
        trappable_imports: true,
        async: true,
        additional_derives: [serde::Serialize, serde::Deserialize],
    });
}

pub mod bindings_metadata_store {
    wash_runtime::wasmtime::component::bindgen!({
        world: "rt-metadata-store",
        trappable_imports: true,
        async: true,
        additional_derives: [serde::Serialize, serde::Deserialize],
    });
}

mod service_binds {
    wash_runtime::wasmtime::component::bindgen!({
        world: "service",
        trappable_imports: true,
        async: true,
    });
}
impl service_binds::townframe::wflow::host::Host for wash_runtime::engine::ctx::Ctx {
    async fn next_op(
        &mut self,
        job_id: service_binds::townframe::wflow::types::JobId,
    ) -> wasmtime::Result<Result<service_binds::townframe::wflow::host::OpState, String>> {
        todo!()
    }

    async fn persist_op(
        &mut self,
        id: service_binds::townframe::wflow::host::OpId,
        res: Vec<u8>,
    ) -> wasmtime::Result<Result<(), String>> {
        todo!()
    }
}

#[derive(Default)]
struct TownframewflowPlugin {
    workloads: DHashMap<Arc<str>, WflowWorkload>,
    keys: DHashMap<Arc<str>, Arc<str>>,
    pending_workloads: DHashMap<Arc<str>, WflowWorkload>,
}

impl TownframewflowPlugin {
    fn check_wflow_interfaces(
        &self,
        interfaces: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

struct WflowWorkload {
    wflow_keys: HashSet<Arc<str>>,
}

#[async_trait::async_trait]
impl wash_runtime::plugin::HostPlugin for TownframewflowPlugin {
    fn id(&self) -> &'static str {
        "townframe:wflow"
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: std::collections::HashSet::from([]),
            imports: std::collections::HashSet::from([
                //
                WitInterface::from("townframe:wflow/host"),
                WitInterface::from("townframe:wflow/partition-host"),
                WitInterface::from("townframe:wflow/metadata-store"),
            ]),
            ..default()
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        info!("XXX starting");
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        interfaces: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        let Some(iface) = interfaces
            .iter()
            .find(|iface| iface.namespace == "townframe" && iface.package == "wflow")
        else {
            unreachable!();
        };
        let Some(wflow_keys_raw) = iface.config.get("wflow_keys") else {
            anyhow::bail!("no wflow_keys defined for townframe:wflow component");
        };
        let wflow_keys: HashSet<Arc<str>> = wflow_keys_raw
            .split(",")
            .map(|key| key.trim().into())
            .collect();
        // FIXME: regex for valid job keys
        if wflow_keys.is_empty() {
            anyhow::bail!("wflow_keys is empty: \"{wflow_keys_raw}\"");
        }
        for key in &wflow_keys {
            if self.keys.contains_key(key) {
                // TODO: include which workload had previously occupied the key
                anyhow::bail!("occupied wflow key: \"{key}\"");
            }
            //self.keys.insert(key.clone(), workload_id.clone());
        }
        let wflow = WflowWorkload { wflow_keys };
        let workload_id: Arc<str> = workload.id().into();
        self.pending_workloads.insert(workload_id, wflow);
        Ok(())
    }
    async fn on_component_bind(
        &self,
        component: &mut wash_runtime::engine::workload::WorkloadComponent,
        _interfaces: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        info!(?component, ?_interfaces, "XXX");
        let Some(host_iface) = _interfaces.iter().find(|iface| {
            iface.namespace == "townframe"
                && iface.package == "wflow"
                && iface.interfaces.contains("host")
        }) else {
            unreachable!();
        };

        service_binds::townframe::wflow::host::add_to_linker(component.linker(), |ctx| ctx)?;
        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        workload: &wash_runtime::engine::workload::ResolvedWorkload,
        _component_id: &str,
    ) -> anyhow::Result<()> {
        info!(?_component_id, "XXX");
        let Some((workload_id, wflow)) = self.pending_workloads.remove(workload.id()) else {
            anyhow::bail!("unrecognized workflow was bound");
        };
        for key in &wflow.wflow_keys {
            if self.keys.contains_key(key) {
                // TODO: include which workload had previously occupied the key
                anyhow::bail!("occupied wflow key: \"{key}\"");
            }
            self.keys.insert(key.clone(), workload_id.clone());
        }
        self.workloads.insert(workload_id, wflow);
        Ok(())
    }
    async fn on_workload_unbind(
        &self,
        workload: &wash_runtime::engine::workload::ResolvedWorkload,
        _interfaces: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        if let Some((_, wflow)) = self.workloads.remove(workload.id()) {
            for key in wflow.wflow_keys {
                self.keys.remove(&key);
            }
        }
        Ok(())
    }
    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test() -> anyhow::Result<()> {
    utils_rs::testing::setup_tracing().unwrap();

    use wash_runtime::host::HostApi;
    use wash_runtime::*;

    // Create a Wasmtime engine
    let engine = engine::Engine::builder().build()?;

    // Configure plugins
    let http_plugin = plugin::wasi_http::HttpServer::new("127.0.0.1:8080".parse()?);
    let runtime_config_plugin = plugin::wasi_config::RuntimeConfig::default();
    let wflow_plugin = TownframewflowPlugin::default();

    // Build and start the host
    let host = host::HostBuilder::new()
        .with_engine(engine)
        .with_plugin(Arc::new(http_plugin))?
        .with_plugin(Arc::new(runtime_config_plugin))?
        .with_plugin(Arc::new(wflow_plugin))?
        .build()?;

    let host = host.start().await?;

    let dbook_wflow_wasm =
        tokio::fs::read("../../target/wasm32-wasip2/debug/daybook_wflows.wasm").await?;

    // Start a workload
    let req = types::WorkloadStartRequest {
        workload: types::Workload {
            namespace: "test".to_string(),
            name: "test-workload".to_string(),
            annotations: std::collections::HashMap::new(),
            service: None,
            components: vec![types::Component {
                bytes: dbook_wflow_wasm.into(),
                ..default()
            }],
            host_interfaces: vec![
                //
                WitInterface {
                    config: [("wflow_keys".to_owned(), "doc-created".to_owned())].into(),
                    ..WitInterface::from("townframe:wflow/host")
                },
            ],
            volumes: vec![],
        },
    };

    host.workload_start(req).await?;

    // tokio::time::sleep(std::time::Duration::from_secs(60)).await;

    Ok(())
}
