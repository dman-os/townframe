use crate::client::NodeClient;
use crate::interlude::*;
use crate::out::out;

use big_nodule_core::protocol::ListNodules;
use big_nodule_core::protocol::NoduleRecord;

/// What the node and this client can say about the node today.
///
/// The node id and the peers come from the node's own `getNodeInfo` operation, and the nodules
/// from `ListNodules`, so both modes answer identically (in process there is no peers service,
/// which the answer says). The place comes from the node directory. The registered services and
/// operations are not exposed by any dispatch operation (the router's table is reachable only in
/// the process that booted it), so they are reported as not exposed.
pub async fn run(as_json: bool) -> Res<ExitCode> {
    let mut out = out();
    let conf = lazy::config().await?;
    let client = lazy::node().await?;
    let info = node_info(&client).await?;

    let nodules = client
        .call(ListNodules {})
        .await
        .wrap_err("listing nodules failed")?
        .nodules;

    let socket_path = conf.node_dir.socket_path()?;
    // Any exit but 0 means "no server serving", the convention `systemctl status` follows.
    let exit = if client.mode() == crate::client::Mode::Serving {
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
        writeln!(out, "{}", serde_json::to_string_pretty(&status)?)?;
        return Ok(exit);
    }
    writeln!(out, "node      {}", info.node_id)?;
    writeln!(out, "path      {}", conf.node_dir.root().display())?;
    writeln!(out, "mode      {}", client.mode().as_str())?;
    writeln!(out, 
        "socket    {} ({})",
        socket_path.display(),
        serde_json::to_value(client.socket_state())?
            .as_str()
            .expect("socket states serialize as strings")
    )?;
    let enabled = nodules.iter().filter(|nodule| nodule.enabled).count();
    writeln!(out, "nodules   {enabled} enabled of {}", nodules.len())?;
    for nodule in &nodules {
        writeln!(out, "{}", nodule_line(nodule))?;
    }
    if conf.node_dir.settings()?.relay {
        writeln!(out, "relay     yes")?;
    }
    writeln!(out, "services  not exposed by the node")?;
    match &info.peers {
        None => writeln!(out, "peers     none attached ({})", client.mode().peers_note())?,
        Some(peers) => {
            writeln!(out, "peers     {} connected", peers.connected.len())?;
            writeln!(out, "  address {}", peers.address)?;
            for peer in &peers.connected {
                writeln!(out, "  {peer}")?;
            }
        }
    }
    Ok(exit)
}

/// Asks the node who it is and what it is connected to.
pub async fn node_info(client: &NodeClient) -> Res<big_node::NodeInfo> {
    client
        .call(big_node::GetNodeInfo {})
        .await
        .wrap_err("getNodeInfo failed")
}

/// One nodule's line in the text status: its cartridge as `<did>/<name>`, whether it is enabled,
/// and its readiness in words.
fn nodule_line(nodule: &NoduleRecord) -> String {
    format!(
        "  {}  {}  {}",
        nodule.cartridge_id,
        if nodule.enabled {
            "enabled"
        } else {
            "disabled"
        },
        nodule.readiness,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_dispatch_core::Readiness;

    fn record(readiness: Readiness) -> NoduleRecord {
        NoduleRecord {
            cartridge_id: "did:key:z6MkvDqGT54cXesYGvABpF1UapVNwjCqRcafi4Px6Thv5T3Z/intxt"
                .parse()
                .unwrap(),
            version: None,
            revision: None,
            actor: None,
            filled_by: None,
            enabled: true,
            readiness,
            artifacts: default(),
            drawers: default(),
            methods: None,
            requests: default(),
            init: None,
            observed_heads: vec![],
            applied_heads: None,
            error: None,
        }
    }

    /// The line names the cartridge by its id and the readiness as a plain word: it once printed
    /// `null` for the id (a stale JSON key) and the readiness as quoted JSON.
    #[test]
    fn a_nodule_line_names_its_cartridge_and_readiness_in_words() {
        assert_eq!(
            nodule_line(&record(Readiness::Ready)),
            "  did:key:z6MkvDqGT54cXesYGvABpF1UapVNwjCqRcafi4Px6Thv5T3Z/intxt  enabled  ready"
        );
        assert_eq!(
            nodule_line(&record(Readiness::NeedsConfiguration)),
            "  did:key:z6MkvDqGT54cXesYGvABpF1UapVNwjCqRcafi4Px6Thv5T3Z/intxt  enabled  needs configuration"
        );
    }
}
