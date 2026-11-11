use crate::client::NodeClient;
use crate::interlude::*;
use method_core::HttpMethod;

use big_nodule_core::protocol::ListNodules;
use method_core::Method;

/// What the node and this client can say about the node today.
///
/// The node id and the peers come from the node's own `getNodeInfo` operation, and the nodules
/// from `ListNodules`, so both modes answer identically (in process there is no peers service,
/// which the answer says). The place comes from the node directory. The registered services and
/// operations are not exposed by any dispatch operation (the router's table is reachable only in
/// the process that booted it), so they are reported as not exposed.
pub async fn run(as_json: bool) -> Res<ExitCode> {
    let conf = lazy::config().await?;
    let client = lazy::node().await?;
    let info = node_info(&client).await?;

    let reply = client
        .call(ListNodules::XRPC.nsid, ListNodules::XRPC.method, &json!({}))
        .await?;
    eyre::ensure!(
        reply.is_success(),
        "listing nodules failed with {}: {}",
        reply.status,
        reply.body
    );
    let nodules = reply.body["nodules"]
        .as_array()
        .ok_or_eyre("listNodules did not answer a `nodules` list")?;

    let socket_path = conf.node_dir.socket_path()?;
    // Any exit but 0 means "no daemon serving", the convention `systemctl status` follows.
    let exit = if client.socket_state() == crate::context::SocketState::Live {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(3)
    };
    if as_json {
        let status = json!({
            "nodeId": info.node_id,
            "path": conf.node_dir.root(),
            "mode": client.mode(),
            "socket": { "path": socket_path, "state": client.socket_state() },
            "nodules": nodules,
            "services": null,
            "peers": info.peers,
            "relay": conf.node_dir.settings()?.relay,
        });
        println!("{}", serde_json::to_string_pretty(&status)?);
        return Ok(exit);
    }
    println!("node      {}", info.node_id);
    println!("path      {}", conf.node_dir.root().display());
    println!("mode      {}", client.mode().as_str());
    println!(
        "socket    {} ({})",
        socket_path.display(),
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
    if conf.node_dir.settings()?.relay {
        println!("relay     yes");
    }
    println!("services  not exposed by the node");
    match &info.peers {
        None => println!("peers     none attached ({})", client.mode().peers_note()),
        Some(peers) => {
            println!("peers     {} connected", peers.connected.len());
            println!("  address {}", peers.address);
            for peer in &peers.connected {
                println!("  {peer}");
            }
        }
    }
    Ok(exit)
}

/// Asks the node who it is and what it is connected to.
pub async fn node_info(client: &NodeClient) -> Res<big_node::NodeInfo> {
    let reply = client
        .call(big_node::GET_NODE_INFO, HttpMethod::Get, &json!({}))
        .await?;
    eyre::ensure!(
        reply.is_success(),
        "getNodeInfo failed with {}: {}",
        reply.status,
        reply.body
    );
    Ok(serde_json::from_value(reply.body)?)
}
