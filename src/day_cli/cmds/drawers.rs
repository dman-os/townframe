use crate::interlude::*;

use big_cabinet_core::DocId;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::GroupKey;
use big_cabinet_core::drawer::listing::KnownDrawers;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::InvitePrincipal;
use big_cabinet_core::drawer::protocol::ListKnownDrawers;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::drawer::protocol::Principal;
use method_core::Method;

#[derive(Debug, clap::Subcommand)]
pub enum DrawersCommands {
    /// List the drawers this node knows
    ///
    /// One drawer per line: its id, and `mirror` when the node mirrors it. The list is the
    /// node's known-drawers list (`listKnownDrawers`), the drawers it was asked to remember.
    Ls {
        /// Print the node's raw answer as JSON
        #[arg(long)]
        json: bool,
    },
    /// Give another node keyhive access to a drawer
    ///
    /// The peer must be known to this node, which it is once the two have connected (see
    /// `day peers add`): its keyhive identity arrives with the connection.
    Invite {
        /// The drawer's id
        drawer: String,
        /// The peer's node id, as `day status` prints it
        peer: String,
        /// What the peer may do: relay stores and forwards ciphertext, read decrypts, write
        /// also changes documents, admin also manages members
        #[arg(long, value_enum)]
        access: AccessArg,
    },
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum AccessArg {
    Relay,
    Read,
    Write,
    Admin,
}

impl From<AccessArg> for DrawerAccess {
    fn from(access: AccessArg) -> Self {
        match access {
            AccessArg::Relay => Self::Relay,
            AccessArg::Read => Self::Read,
            AccessArg::Write => Self::Write,
            AccessArg::Admin => Self::Admin,
        }
    }
}

pub async fn run(command: DrawersCommands) -> Res<ExitCode> {
    match command {
        DrawersCommands::Ls { json } => ls(json).await,
        DrawersCommands::Invite {
            drawer,
            peer,
            access,
        } => invite(&drawer, &peer, access.into()).await,
    }
}

/// Learns the known-drawers drawer's id from `getNodeInfo`, mints an admin handle on just that
/// drawer (what `listKnownDrawers` authorizes) for this one call, and lists through it.
async fn ls(as_json: bool) -> Res<ExitCode> {
    let client = lazy::node().await?;
    let info = super::status::node_info(&client).await?;

    let minted = client
        .call(
            MintDrawerCapability::XRPC.nsid,
            &serde_json::to_value(MintDrawerCapability {
                scope: DrawerScope {
                    drawers: [(info.known_drawers_id, DrawerAccess::Admin)].into(),
                    create: false,
                },
                rights: Rights::USE,
            })?,
        )
        .await?;
    eyre::ensure!(
        minted.is_success(),
        "minting a handle on the known-drawers list failed with {}: {}",
        minted.status,
        minted.body
    );
    let capability = serde_json::from_value(minted.body["capabilityId"].clone())
        .wrap_err("the node's answer carries no capability id")?;

    let reply = client
        .call(
            ListKnownDrawers::XRPC.nsid,
            &serde_json::to_value(ListKnownDrawers { capability })?,
        )
        .await?;
    eyre::ensure!(
        reply.is_success(),
        "listing the known drawers failed with {}: {}",
        reply.status,
        reply.body
    );
    if as_json {
        println!("{}", serde_json::to_string_pretty(&reply.body)?);
        return Ok(ExitCode::SUCCESS);
    }
    let known: KnownDrawers = serde_json::from_value(reply.body)?;
    for drawer in known.drawers {
        if drawer.mirror {
            println!("{}  mirror", drawer.drawer_id);
        } else {
            println!("{}", drawer.drawer_id);
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// Mints an admin handle on the drawer for this one call, then invites the peer through it. The
/// node refuses a peer it has not met, which this says in the terms of the commands that fix it.
async fn invite(drawer: &str, peer: &str, access: DrawerAccess) -> Res<ExitCode> {
    let drawer_id: DocId = drawer
        .parse()
        .map_err(|error| eyre::eyre!("`{drawer}` is not a drawer id: {error}"))?;
    let peer_bytes: [u8; 32] = data_encoding::HEXLOWER
        .decode(peer.as_bytes())
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| {
            eyre::eyre!(
                "`{peer}` is not a node id (64 lowercase hex digits, as `day status` prints)"
            )
        })?;
    let client = lazy::node().await?;

    let minted = client
        .call(
            MintDrawerCapability::XRPC.nsid,
            &serde_json::to_value(MintDrawerCapability {
                scope: DrawerScope {
                    drawers: [(drawer_id.clone(), DrawerAccess::Admin)].into(),
                    create: false,
                },
                rights: Rights::USE,
            })?,
        )
        .await?;
    eyre::ensure!(
        minted.is_success(),
        "minting a handle on the drawer failed with {}: {}",
        minted.status,
        minted.body
    );
    let capability = serde_json::from_value(minted.body["capabilityId"].clone())
        .wrap_err("the node's answer carries no capability id")?;

    let reply = client
        .call(
            InvitePrincipal::XRPC.nsid,
            &serde_json::to_value(InvitePrincipal {
                capability,
                drawer_id,
                principal: Principal::Peer {
                    peer_id: GroupKey(peer_bytes),
                },
                access,
            })?,
        )
        .await?;
    if reply.is_success() {
        println!("invited {peer} to {drawer} at {access:?}");
        return Ok(ExitCode::SUCCESS);
    }
    if reply.body["error"] == "UnknownPeer" {
        error!(
            "this node has not met {peer}: connect to it first (`day peers add <ticket>` and a daemon restart), then retry"
        );
    } else {
        error!(
            status = reply.status,
            "the node refused the invite: {}: {}",
            reply.body["error"].as_str().unwrap_or("unnamed error"),
            reply.body["message"].as_str().unwrap_or_default()
        );
    }
    Ok(ExitCode::FAILURE)
}
