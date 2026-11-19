use crate::interlude::*;
use std::collections::HashMap;
use std::sync::Arc;

mod plugins;
use plugins::MailPlugin;
use plugins::ServicePlugin;
use wash_plugin_sqlite::SqlPlugin;
use wash_runtime::engine::Engine;
use wash_runtime::host::HostApi;
use wash_runtime::host::HostBuilder;
use wash_runtime::host::http::DevRouter;
use wash_runtime::host::http::Ingress;
use wash_runtime::types::Component;
use wash_runtime::types::HostPathVolume;
use wash_runtime::types::LocalResources;
use wash_runtime::types::Volume;
use wash_runtime::types::VolumeMount;
use wash_runtime::types::VolumeType;
use wash_runtime::types::Workload;
use wash_runtime::types::WorkloadStartRequest;
use wash_runtime::types::WorkloadStopRequest;
use wash_runtime::wit::WitInterface;

/// Run the `btress_auth` wash host (playground).
///
/// Builds a wash host with the capability plugins `btress_auth` needs
/// (`townframe:sqlite/sqlite-connection` via [`SqlPlugin`] and
/// `townframe:api-utils/http-service` via [`ServicePlugin`]), starts the
/// `btress_auth` component (which exports `wasi:http/incoming-handler`), and
/// serves it on `0.0.0.0:8071`.
///
/// Playground: the sqlite path is supplied to [`ServicePlugin`] at boot and the
/// component env vars are hardcoded below — not config-driven yet.
pub async fn run() -> Res<ExitCode> {
    let config = crate::lazy::config().await?;
    run_inner(Arc::clone(&config.sqlite_runtime))
        .await
        .map_err(|err| eyre::eyre!(err.to_string()))
}

/// Where the btress_sysadmin workspace writes cargo-leptos output: `CARGO_TARGET_DIR` when
/// the dev shell scoped it per worktree, otherwise the crate workspace's own `target/`.
fn wasm_workspace_target_dir() -> std::path::PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../btress_sysadmin/target")
        })
}

fn sysadmin_host_interfaces() -> Vec<WitInterface> {
    vec![
        WitInterface::from("wasi:http/handler@0.3.0"),
        WitInterface::from("wasi:random/random@0.3.0"),
    ]
}

async fn run_inner(
    sqlite_runtime: Arc<sqlite_rs::SqliteRuntime>,
) -> wash_runtime::wasmtime::anyhow::Result<ExitCode> {
    // ---- btress_auth host (port 8071) ----
    let auth_engine = Engine::builder().with_pooling_allocator(true).build()?;
    let auth_http_handler = Ingress::new(DevRouter::default(), "0.0.0.0:8071".parse()?).await?;
    let auth_host = HostBuilder::new()
        .with_engine(auth_engine)
        .with_friendly_name("btress-auth-host")
        .with_http_handler(Arc::new(auth_http_handler))
        .with_plugin(Arc::new(SqlPlugin::new()))?
        .with_plugin(Arc::new(ServicePlugin::new(
            sqlite_runtime,
            std::env::temp_dir().join("townframe-auth.sqlite"),
        )))?
        .with_plugin(Arc::new(MailPlugin::new()))?
        .with_plugin(Arc::new(
            wash_runtime::plugin::wasi_logging::TracingLogger::default(),
        ))?
        .build()?;
    let auth_host = auth_host.start().await?;
    println!("Host started: {}", auth_host.friendly_name());

    // Load the btress_auth component from disk
    let auth_component_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../btress_auth/dist/btress_auth.wasm");
    let auth_component_bytes = std::fs::read(&auth_component_path)?;

    // Start the btress_auth workload
    let auth_workload_id = "btress-auth".to_string();
    let auth_request = WorkloadStartRequest {
        workload_id: auth_workload_id.clone(),
        workload: Workload {
            namespace: "default".to_string(),
            name: "btress-auth".to_string(),
            annotations: HashMap::new(),
            service: None,
            components: vec![Component {
                name: "btress-auth".to_string(),
                bytes: auth_component_bytes.into(),
                local_resources: LocalResources {
                    environment: HashMap::from([
                        (
                            "BTRESS_URL".to_string(),
                            "http://localhost:8071".to_string(),
                        ),
                        ("ROOT_WEB_DOMAIN".to_string(), "localhost".to_string()),
                        (
                            "BETTER_AUTH_URL".to_string(),
                            "http://localhost:8071".to_string(),
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
                ..Component::default()
            }],
            host_interfaces: vec![
                WitInterface::from("townframe:api-utils/http-service"),
                WitInterface::from("townframe:sqlite/sqlite-connection"),
                WitInterface::from("townframe:api-utils/mail"),
                // Required for the ingress to route HTTP to this workload.
                WitInterface::from("wasi:http/handler@0.3.0"),
                // The component imports wasi:logging (jco keeps unused world imports);
                // the TracingLogger plugin provides it when declared here.
                WitInterface::from("wasi:logging/logging@0.1.0-draft"),
            ],
            volumes: vec![],
        },
    };

    let auth_response = auth_host.workload_start(auth_request).await?;
    let auth_workload_id = auth_response.workload_status.workload_id.clone();
    println!("Workload started: {}", auth_workload_id);

    // ---- btress_sysadmin host (port 3000) ----
    let sysadmin_engine = Engine::builder().with_pooling_allocator(true).build()?;
    let sysadmin_http_handler = Ingress::new(DevRouter::default(), "0.0.0.0:3000".parse()?).await?;
    let sysadmin_host = HostBuilder::new()
        .with_engine(sysadmin_engine)
        .with_friendly_name("btress-sysadmin-host")
        .with_http_handler(Arc::new(sysadmin_http_handler))
        .build()?;
    let sysadmin_host = sysadmin_host.start().await?;
    println!("Host started: {}", sysadmin_host.friendly_name());

    let target_dir = wasm_workspace_target_dir();
    let component_path = target_dir.join("wasm/wasm32-wasip3/wasm-release/btress_sysadmin.wasm");
    let component_bytes = std::fs::read(&component_path)?;
    let site_dir = target_dir.join("site");

    // Start the btress_sysadmin workload (mirrors the `.wash/config.yaml` dev
    // setup: site assets volume-mounted at /public + LEPTOS env vars)
    let sysadmin_workload_id = "btress-sysadmin".to_string();
    let sysadmin_request = WorkloadStartRequest {
        workload_id: sysadmin_workload_id.clone(),
        workload: Workload {
            namespace: "default".to_string(),
            name: "btress-sysadmin".to_string(),
            annotations: HashMap::new(),
            service: None,
            components: vec![Component {
                name: "btress-sysadmin".to_string(),
                bytes: component_bytes.into(),
                local_resources: LocalResources {
                    environment: HashMap::from([
                        (
                            "LEPTOS_OUTPUT_NAME".to_string(),
                            "btress_sysadmin".to_string(),
                        ),
                        ("LEPTOS_SITE_ADDR".to_string(), "127.0.0.1:3000".to_string()),
                        ("RUST_BACKTRACE".to_string(), "1".to_string()),
                    ]),
                    volume_mounts: vec![VolumeMount {
                        name: "public".to_string(),
                        mount_path: "/public".to_string(),
                        read_only: true,
                    }],
                    ..LocalResources::default()
                },
                pool_size: 1,
                max_invocations: 0,
                ..Component::default()
            }],
            host_interfaces: sysadmin_host_interfaces(),
            volumes: vec![Volume {
                name: "public".to_string(),
                volume_type: VolumeType::HostPath(HostPathVolume {
                    local_path: site_dir.to_string_lossy().to_string(),
                }),
            }],
        },
    };

    let sysadmin_response = sysadmin_host.workload_start(sysadmin_request).await?;
    let sysadmin_workload_id = sysadmin_response.workload_status.workload_id.clone();
    println!("Workload started: {}", sysadmin_workload_id);

    // Keep the hosts running
    println!(
        "Host listening on http://0.0.0.0:8071 (btress_auth) and http://0.0.0.0:3000 (btress_sysadmin)"
    );
    tokio::signal::ctrl_c().await?;

    // Clean shutdown (reverse of construction order)
    sysadmin_host
        .workload_stop(WorkloadStopRequest {
            workload_id: sysadmin_workload_id,
        })
        .await?;
    sysadmin_host.stop().await?;
    auth_host
        .workload_stop(WorkloadStopRequest {
            workload_id: auth_workload_id,
        })
        .await?;
    auth_host.stop().await?;
    println!("Host shutdown complete");
    Ok(ExitCode::SUCCESS)
}
