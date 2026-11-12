use crate::interlude::*;
use big_node_holder::SocketState;

#[derive(Debug, clap::Subcommand)]
pub enum PeersCommands {
    /// Add a peer to the daemon's bootstrap list: it is dialed as soon as the daemon starts
    Add {
        /// The peer's endpoint ticket, as `day node status` prints it under `address`
        ticket: String,
    },
    /// List the bootstrap peers and, when a daemon is serving, who it is connected to
    Ls,
}

pub async fn run(command: PeersCommands) -> Res<ExitCode> {
    match command {
        PeersCommands::Add { ticket } => add(&ticket).await,
        PeersCommands::Ls => ls().await,
    }
}

/// Records the peer in `node.json`. The settings are read once, when a daemon boots, so this
/// never reaches into a running node: a serving daemon is told that it has to restart to dial
/// the new peer. Adding the first peer also creates the `peers` section, which is what makes the
/// daemon attach the network endpoint at all.
async fn add(ticket: &str) -> Res<ExitCode> {
    let dir = &*lazy::node_dir().await?;
    let changed = dir.update_settings(|settings| {
        settings
            .peers
            .get_or_insert_with(default)
            .add_bootstrap(ticket)
    })?;
    println!(
        "{}",
        if changed {
            "added to the bootstrap list"
        } else {
            "already in the bootstrap list"
        }
    );
    if changed && dir.probe_socket().await? == SocketState::Live {
        println!("the running daemon dials it after `day node stop` and `day node start`");
    }
    Ok(ExitCode::SUCCESS)
}

async fn ls() -> Res<ExitCode> {
    let settings = lazy::node_dir().await?.settings()?;
    let bootstrap = settings
        .peers
        .as_ref()
        .map_or(&[][..], |peers| peers.bootstrap.as_slice());
    println!("bootstrap {}", bootstrap.len());
    for ticket in bootstrap {
        println!("  {ticket}");
    }
    let client = lazy::node().await?;
    match crate::cmds::status::node_info(&client).await?.peers {
        None => println!("connected none attached ({})", client.mode().peers_note()),
        Some(peers) => {
            println!("connected {}", peers.connected.len());
            for peer in &peers.connected {
                println!("  {peer}");
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}
