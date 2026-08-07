/*
use wash_runtime::{
    engine::Engine,
    host::{HostBuilder, HostApi, http::{Ingress, DevRouter}},
    plugin::wasi_config::DynamicConfig,
    types::{Workload, WorkloadStartRequest, Component, LocalResources},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing for observability
    tracing_subscriber::fmt::init();

    Ok(())
}
*/
use crate::interlude::*;
pub async fn run() -> Res<ExitCode> {
    // Create the engine with pooling enabled
    let engine = Engine::builder().with_pooling_allocator(true).build()?;

    // Configure HTTP handler and plugins
    let http_handler = Ingress::new(DevRouter::default(), "0.0.0.0:8080".parse()?).await?;
    let config_plugin = DynamicConfig::new(false);

    // Build and start the host
    let host = HostBuilder::new()
        .with_engine(engine)
        .with_friendly_name("my-custom-host")
        .with_http_handler(Arc::new(http_handler))
        .with_plugin(Arc::new(config_plugin))?
        .build()?;

    let host = host.start().await?;
    println!("Host started: {}", host.friendly_name());

    // Load a component from disk
    let component_bytes = std::fs::read("./my-component.wasm")?;

    // Create and start a workload
    let request = WorkloadStartRequest {
        workload_id: uuid::Uuid::new_v4().to_string(),
        workload: Workload {
            namespace: "default".to_string(),
            name: "my-component".to_string(),
            annotations: HashMap::new(),
            service: None,
            components: vec![Component {
                name: "my-component".to_string(),
                bytes: component_bytes.into(),
                digest: None,
                local_resources: LocalResources::default(),
                pool_size: 5,
                max_invocations: 0,
            }],
            host_interfaces: vec![],
            volumes: vec![],
        },
    };

    let response = host.workload_start(request).await?;
    let workload_id = response.workload_status.workload_id.clone();
    println!("Workload started: {}", workload_id);

    // Keep the host running
    println!("Host listening on http://0.0.0.0:8080");
    tokio::signal::ctrl_c().await?;

    // Clean shutdown
    host.workload_stop(wash_runtime::types::WorkloadStopRequest { workload_id })
        .await?;

    host.stop().await?;
    println!("Host shutdown complete");
    Ok(ExitCode::SUCCESS)
}
