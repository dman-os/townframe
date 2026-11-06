use crate::interlude::*;

use big_nodule_core::protocol::ListNodules;
use service_utils_rs::Operation;

/// What the node and this client can say about the node today.
///
/// The node id and place come from the node directory, and the nodules from the node's own
/// `ListNodules` operation, so both modes answer identically. The registered services and
/// operations and the connected peers are not exposed by any dispatch operation (the router's
/// table and the peers service are reachable only in the process that booted them), so they are
/// reported as not exposed instead of being read from an in-process node in one mode only.
pub async fn run(as_json: bool) -> Res<ExitCode> {
    let conf = lazy::config().await?;
    let settings = conf.node_dir.settings()?;
    let client = lazy::node().await?;

    let reply = client.call(ListNodules::XRPC.nsid, &json!({})).await?;
    eyre::ensure!(
        reply.is_success(),
        "listing nodules failed with {}: {}",
        reply.status,
        reply.body
    );
    let nodules = reply
        .body
        .as_array()
        .ok_or_eyre("listNodules did not answer a list")?;

    if as_json {
        let socket_path = conf.node_dir.socket_path();
        let status = json!({
            "nodeId": settings.node_id(),
            "path": conf.node_dir.root(),
            "mode": client.mode(),
            "socket": { "path": socket_path, "state": client.socket_state() },
            "nodules": nodules,
            "services": null,
            "peers": null,
        });
        println!("{}", serde_json::to_string_pretty(&status)?);
        return Ok(ExitCode::SUCCESS);
    }
    println!("node      {}", settings.node_id());
    println!("path      {}", conf.node_dir.root().display());
    println!("mode      {}", client.mode().as_str());
    println!(
        "socket    {} ({})",
        conf.node_dir.socket_path().display(),
        serde_json::to_value(client.socket_state())?
            .as_str()
            .expect("socket states serialize as strings")
    );
    let enabled = nodules
        .iter()
        .filter(|nodule| nodule["enabled"] == json!(true))
        .count();
    println!("nodules   {enabled} enabled of {}", nodules.len());
    for nodule in nodules {
        println!(
            "  {}  {}  {}",
            nodule["plugId"],
            if nodule["enabled"] == json!(true) {
                "enabled"
            } else {
                "disabled"
            },
            nodule["readiness"],
        );
    }
    println!("services  not exposed by the node");
    println!("peers     not exposed by the node");
    Ok(ExitCode::SUCCESS)
}
