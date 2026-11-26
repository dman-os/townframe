//! Runs `intxt_web.wasm` (the viewer, a `wasi:http/handler@0.3` component) as the one workload of a
//! wash_runtime host that serves it on a TCP port.
//!
//! The viewer is kept apart from `big_node`: it is a client of a node like any other, so the host
//! gives it three things and nothing else: the socket, the environment `intxt_web::node` reads
//! (`INTXT_WEB_NODE` and its capabilities), and an egress allowlist holding exactly the node's
//! scheme and authority. Given none of the node flags the component serves its built-in fixture.

use clap::Parser;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use utils_rs::prelude::*;
use wash_runtime::engine::Engine;
use wash_runtime::host::HostApi;
use wash_runtime::host::HostBuilder;
use wash_runtime::host::allowed_hosts::AllowedHost;
use wash_runtime::host::http::DevRouter;
use wash_runtime::host::http::Ingress;
use wash_runtime::types::Component;
use wash_runtime::types::LocalResources;
use wash_runtime::types::Workload;
use wash_runtime::types::WorkloadStartRequest;
use wash_runtime::types::WorkloadStopRequest;
use wash_runtime::wit::WitInterface;

#[derive(Debug, Parser)]
#[command(about = "Serve the intxt web viewer from a wash_runtime host")]
struct Args {
    /// The viewer component, `intxt_web.wasm`.
    #[arg(long)]
    component: PathBuf,
    /// Where to listen.
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,
    /// The node to read intxts from, such as `http://127.0.0.1:8080`. Without it (and the three
    /// capabilities) the viewer serves its built-in fixture.
    #[arg(long, requires_all = ["doc_cap", "blob_cap", "dispatch_cap"])]
    node: Option<String>,
    /// The doc capability the viewer reads datums with (`INTXT_WEB_DOC_CAP`).
    #[arg(long, requires = "node")]
    doc_cap: Option<String>,
    /// The blob capability (`INTXT_WEB_BLOB_CAP`).
    #[arg(long, requires = "node")]
    blob_cap: Option<String>,
    /// The dispatch capability every request carries as its bearer (`INTXT_WEB_DISPATCH_CAP`).
    #[arg(long, requires = "node")]
    dispatch_cap: Option<String>,
}

/// The wash API reports `anyhow` errors; the rest of the repo reports eyre ones.
fn wash<T>(result: wash_runtime::wasmtime::anyhow::Result<T>) -> Res<T> {
    result.map_err(|err| eyre::eyre!("{err:#}"))
}

impl Args {
    /// The environment `intxt_web::node::NodeConfig::from_env` reads, and the egress policy that
    /// goes with it: the node's scheme and authority and nothing else. Both are empty without a
    /// node, and an empty allowlist denies all outgoing requests.
    fn node_resources(&self) -> Res<LocalResources> {
        let (Some(node), Some(doc), Some(blob), Some(bearer)) = (
            &self.node,
            &self.doc_cap,
            &self.blob_cap,
            &self.dispatch_cap,
        ) else {
            return Ok(LocalResources::default());
        };
        let uri: http::Uri = node
            .parse()
            .wrap_err_with(|| format!("--node {node:?} is not a URL"))?;
        let (Some(scheme), Some(authority)) = (uri.scheme_str(), uri.authority()) else {
            eyre::bail!(
                "--node {node:?} needs a scheme and an authority, such as http://127.0.0.1:8080"
            );
        };
        let allowed: AllowedHost = wash(format!("{scheme}://{authority}").parse())?;
        Ok(LocalResources {
            environment: HashMap::from([
                ("INTXT_WEB_NODE".to_owned(), node.clone()),
                ("INTXT_WEB_DOC_CAP".to_owned(), doc.clone()),
                ("INTXT_WEB_BLOB_CAP".to_owned(), blob.clone()),
                ("INTXT_WEB_DISPATCH_CAP".to_owned(), bearer.clone()),
            ]),
            allowed_hosts: Arc::from([allowed]),
            ..LocalResources::default()
        })
    }
}

#[tokio::main]
async fn main() -> Res<()> {
    utils_rs::tracing::setup_tracing()?;
    let args = Args::parse();
    let local_resources = args.node_resources()?;
    let bytes = std::fs::read(&args.component)
        .wrap_err_with(|| format!("reading {}", args.component.display()))?;

    let engine = wash(Engine::builder().with_pooling_allocator(true).build())?;
    let ingress = wash(Ingress::new(DevRouter::default(), args.bind).await)?;
    let host = wash(
        HostBuilder::new()
            .with_engine(engine)
            .with_friendly_name("intxt-web-host")
            .with_http_handler(Arc::new(ingress))
            .build(),
    )?;
    let host = wash(host.start().await)?;

    let started = wash(
        host.workload_start(WorkloadStartRequest {
            workload_id: "intxt-web".to_owned(),
            workload: Workload {
                namespace: "default".to_owned(),
                name: "intxt-web".to_owned(),
                annotations: HashMap::new(),
                service: None,
                components: vec![Component {
                    name: "intxt-web".to_owned(),
                    bytes: bytes.into(),
                    local_resources,
                    pool_size: 1,
                    max_invocations: 0,
                    ..Component::default()
                }],
                // The ingress routes only to a workload that declares the handler. Outgoing
                // HTTP needs no entry: the engine links `wasi:http` for every workload, and
                // `allowed_hosts` is what admits a destination.
                host_interfaces: vec![
                    WitInterface::from("wasi:http/handler@0.3.0"),
                    WitInterface::from("wasi:random/random@0.3.0"),
                ],
                volumes: vec![],
            },
        })
        .await,
    )?;
    let workload_id = started.workload_status.workload_id;
    tracing::info!(bind = %args.bind, "serving the intxt viewer");

    wait_for_shutdown().await?;

    // Reverse of construction order.
    wash(
        host.workload_stop(WorkloadStopRequest { workload_id })
            .await,
    )?;
    wash(host.stop().await)?;
    Ok(())
}

/// Ctrl-C, or SIGTERM from a supervisor.
async fn wait_for_shutdown() -> Res<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = terminate.recv() => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(node: Option<&str>) -> Args {
        let cap = || node.map(|_| "00000000-0000-0000-0000-000000000001".to_owned());
        Args {
            component: PathBuf::from("intxt_web.wasm"),
            bind: "127.0.0.1:0".parse().unwrap(),
            node: node.map(str::to_owned),
            doc_cap: cap(),
            blob_cap: cap(),
            dispatch_cap: cap(),
        }
    }

    #[test]
    fn egress_reaches_the_node_and_nothing_else() {
        let resources = args(Some("http://127.0.0.1:8080/ignored/path"))
            .node_resources()
            .unwrap();
        let admits = |uri: &str| {
            let uri: http::Uri = uri.parse().unwrap();
            resources
                .allowed_hosts
                .iter()
                .any(|host| host.matches(&uri))
        };
        assert!(admits("http://127.0.0.1:8080/xrpc/anything"));
        assert!(!admits("http://127.0.0.1:8081/"));
        assert!(!admits("https://127.0.0.1:8080/"));
        assert!(!admits("http://example.com:8080/"));
        assert_eq!(
            resources.environment["INTXT_WEB_NODE"],
            "http://127.0.0.1:8080/ignored/path"
        );
        assert_eq!(resources.environment.len(), 4);
    }

    #[test]
    fn with_no_node_the_viewer_gets_no_environment_and_no_egress() {
        let resources = args(None).node_resources().unwrap();
        assert!(resources.environment.is_empty());
        assert!(resources.allowed_hosts.is_empty());
    }

    #[test]
    fn a_node_without_its_capabilities_is_refused_by_the_flags() {
        assert!(Args::try_parse_from(["h", "--component", "c", "--node", "http://x:1"]).is_err());
        assert!(Args::try_parse_from(["h", "--component", "c", "--doc-cap", "a"]).is_err());
    }

    #[test]
    fn a_node_url_without_a_scheme_and_authority_is_refused() {
        assert!(args(Some("/just/a/path")).node_resources().is_err());
    }
}
