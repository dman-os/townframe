use crate::interlude::*;
use crate::out::out;
use day_node::SocketState;

#[derive(Debug, clap::Subcommand)]
pub enum PeersCommands {
    /// Add a peer to the server's bootstrap list: it is dialed as soon as the server starts
    Add {
        /// The peer's endpoint ticket, as `day node status` prints it under `address`
        ticket: String,
    },
    /// List the bootstrap peers and, when a server is serving, who it is connected to
    Ls {
        /// Print JSON instead of text
        #[arg(long)]
        json: bool,
    },
}

pub async fn run(command: PeersCommands) -> Res<ExitCode> {
    match command {
        PeersCommands::Add { ticket } => add(&ticket).await,
        PeersCommands::Ls { json } => ls(json).await,
    }
}

/// Records the peer in `node.json`. The settings are read once, when a server boots, so this
/// never reaches into a running node: a serving server is told that it has to restart to dial
/// the new peer. Adding the first peer also creates the `peers` section, which is what makes the
/// server attach the network endpoint at all.
async fn add(ticket: &str) -> Res<ExitCode> {
    let mut out = out();
    let conf = lazy::config().await?;
    let dir = &conf.node_dir;
    let changed = dir
        .update_settings(|settings| {
            settings
                .peers
                .get_or_insert_with(default)
                .add_bootstrap(ticket)
        })
        .await?;
    writeln!(
        out,
        "{}",
        if changed {
            "added to the bootstrap list"
        } else {
            "already in the bootstrap list"
        }
    )?;
    if changed && dir.probe_socket().await? == SocketState::Live {
        writeln!(
            out,
            "the running server dials it after `day node stop` and `day node start --bg`"
        )?;
    }
    Ok(ExitCode::SUCCESS)
}

async fn ls(as_json: bool) -> Res<ExitCode> {
    let mut out = out();
    let conf = lazy::config().await?;
    let settings = conf.node_dir.settings()?;
    let bootstrap = settings
        .peers
        .as_ref()
        .map_or(&[][..], |peers| peers.bootstrap.as_slice());
    let client = lazy::node().await?;
    let peers = crate::cmds::status::node_info(&client).await?.peers;
    if as_json {
        let listed = json!({ "bootstrap": bootstrap, "peers": peers });
        writeln!(out, "{}", serde_json::to_string_pretty(&listed)?)?;
        return Ok(ExitCode::SUCCESS);
    }
    writeln!(out, "bootstrap {}", bootstrap.len())?;
    for ticket in bootstrap {
        writeln!(out, "  {ticket}")?;
    }
    match peers {
        None => writeln!(
            out,
            "connected none attached ({})",
            client.mode().peers_note()
        )?,
        Some(peers) => {
            writeln!(out, "connected {}", peers.connected.len())?;
            for peer in &peers.connected {
                writeln!(out, "  {peer}")?;
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}
