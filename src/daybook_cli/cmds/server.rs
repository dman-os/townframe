use crate::interlude::*;
use std::collections::HashMap;
use std::sync::Arc;

use daybook_core::rt::wash_plugin::ServicePlugin;
use wash_plugin_sqlite::SqlPlugin;
use wash_runtime::{
    engine::Engine,
    host::{HostApi, HostBuilder, http::{DevRouter, Ingress}},
    types::{Component, LocalResources, Workload, WorkloadStartRequest, WorkloadStopRequest},
    wit::WitInterface,
};

/// Run the `btress_auth` wash host (playground).
///
/// Builds a wash host with the capability plugins `btress_auth` needs
/// (`townframe:sqlite/sqlite-connection` via [`SqlPlugin`] and
/// `townframe:api-utils/http-service` via [`ServicePlugin`]), starts the
/// `btress_auth` component (which exports `wasi:http/incoming-handler`), and
/// serves it on `0.0.0.0:8080`.
///
/// Playground: the sqlite path is hardcoded inside [`ServicePlugin`] and the
/// component env vars are hardcoded below — not config-driven yet.
pub async fn run() -> Res<ExitCode> {
    run_inner()
        .await
        .map_err(|err| eyre::eyre!(err.to_string()))
}

async fn run_inner() -> wash_runtime::wasmtime::anyhow::Result<ExitCode> {
    // Create the engine with pooling enabled
    let engine = Engine::builder().with_pooling_allocator(true).build()?;

    // Configure the HTTP ingress (btress_auth exports wasi:http/incoming-handler)
    let http_handler = Ingress::new(DevRouter::default(), "0.0.0.0:8080".parse()?).await?;

    // Build and start the host with the capability plugins btress_auth needs
    let host = HostBuilder::new()
        .with_engine(engine)
        .with_friendly_name("btress-auth-host")
        .with_http_handler(Arc::new(http_handler))
        .with_plugin(Arc::new(SqlPlugin::new()))?
        .with_plugin(Arc::new(ServicePlugin::new()))?
        .build()?;

    let host = host.start().await?;
    println!("Host started: {}", host.friendly_name());

    // Load the btress_auth component from disk
    let component_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../btress_auth/dist/btress_auth.wasm");
    let component_bytes = std::fs::read(&component_path)?;

    // Start the btress_auth workload
    let workload_id = "btress-auth".to_string();
    let request = WorkloadStartRequest {
        workload_id: workload_id.clone(),
        workload: Workload {
            namespace: "default".to_string(),
            name: "btress-auth".to_string(),
            annotations: HashMap::new(),
            service: None,
            components: vec![Component {
                name: "btress-auth".to_string(),
                bytes: component_bytes.into(),
                digest: None,
                local_resources: LocalResources {
                    environment: HashMap::from([
                        ("BTRESS_URL".to_string(), "http://localhost:8080".to_string()),
                        (
                            "ROOT_WEB_DOMAIN".to_string(),
                            "localhost".to_string(),
                        ),
                        (
                            "BETTER_AUTH_URL".to_string(),
                            "http://localhost:8080".to_string(),
                        ),
                        (
                            "BETTER_AUTH_SECRET".to_string(),
                            "dev-secret-change-me".to_string(),
                        ),
                    ]),
                    ..LocalResources::default()
                },
                pool_size: 1,
                max_invocations: 0,
            }],
            host_interfaces: vec![
                WitInterface::from("townframe:api-utils/http-service"),
                WitInterface::from("townframe:sqlite/sqlite-connection"),
            ],
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
    host.workload_stop(WorkloadStopRequest { workload_id })
        .await?;
    host.stop().await?;
    println!("Host shutdown complete");
    Ok(ExitCode::SUCCESS)
}
