use crate::interlude::*;

use big_cabinet_core::DocId;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::GroupKey;
use big_cabinet_core::drawer::listing::KnownDrawers;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::ForgetDrawer;
use big_cabinet_core::drawer::protocol::InvitePrincipal;
use big_cabinet_core::drawer::protocol::ListKnownDrawers;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::drawer::protocol::Principal;
use big_cabinet_core::drawer::protocol::RememberDrawer;
use big_cabinet_core::drawer::protocol::RevokePrincipal;
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
    /// Remember a drawer, by default mirroring it
    ///
    /// A node syncs only the drawers it mirrors: being made a member of a drawer on another node
    /// lets this node read it, but nothing is fetched until this node mirrors it (a relay is the
    /// exception: it mirrors every drawer it is made a member of). Remembering an already
    /// remembered drawer updates its mirror setting.
    Remember {
        /// The drawer's id
        drawer: String,
        /// Keep it in the list without syncing its documents
        #[arg(long)]
        no_mirror: bool,
    },
    /// Forget a drawer: drop it from the known-drawers list and stop mirroring it
    ///
    /// The documents already held stay; this node just stops asking peers for the drawer.
    Forget {
        /// The drawer's id
        drawer: String,
    },
    /// Add another node as a member of a drawer, with keyhive access
    ///
    /// The peer must be known to this node, which it is once the two have connected (see
    /// `day peers add`): its keyhive identity arrives with the connection.
    AddMember {
        /// The drawer's id
        drawer: String,
        /// The peer's node id, as `day node status` prints it
        peer: String,
        /// What the peer may do: relay stores and forwards ciphertext, read decrypts, write
        /// also changes documents, admin also manages members
        #[arg(long, value_enum)]
        access: AccessArg,
    },
    /// Remove another node's membership of a drawer
    RevokeMember {
        /// The drawer's id
        drawer: String,
        /// The peer's node id, as `day node status` prints it
        peer: String,
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
        DrawersCommands::Remember { drawer, no_mirror } => remember(&drawer, !no_mirror).await,
        DrawersCommands::Forget { drawer } => forget(&drawer).await,
        DrawersCommands::AddMember {
            drawer,
            peer,
            access,
        } => add_member(&drawer, &peer, access.into()).await,
        DrawersCommands::RevokeMember { drawer, peer } => revoke_member(&drawer, &peer).await,
    }
}

/// Lists the known-drawers list through an admin handle on just that drawer.
async fn ls(as_json: bool) -> Res<ExitCode> {
    let client = lazy::node().await?;
    let capability = known_drawers_handle(&client).await?;

    let reply = client
        .call(
            ListKnownDrawers::XRPC.nsid,
            ListKnownDrawers::XRPC.method,
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

/// Remembers `drawer` on this node's known-drawers list, mirrored or not.
async fn remember(drawer: &str, mirror: bool) -> Res<ExitCode> {
    let client = lazy::node().await?;
    let drawer_id = parse_drawer(drawer)?;
    let capability = known_drawers_handle(&client).await?;
    let reply = client
        .call(
            RememberDrawer::XRPC.nsid,
            RememberDrawer::XRPC.method,
            &serde_json::to_value(RememberDrawer {
                capability,
                drawer_id,
                mirror,
            })?,
        )
        .await?;
    eyre::ensure!(
        reply.is_success(),
        "remembering the drawer failed with {}: {}",
        reply.status,
        reply.body
    );
    if mirror {
        println!("remembered {drawer}, mirrored");
    } else {
        println!("remembered {drawer}, not mirrored");
    }
    Ok(ExitCode::SUCCESS)
}

/// Drops `drawer` from this node's known-drawers list.
async fn forget(drawer: &str) -> Res<ExitCode> {
    let client = lazy::node().await?;
    let drawer_id = parse_drawer(drawer)?;
    let capability = known_drawers_handle(&client).await?;
    let reply = client
        .call(
            ForgetDrawer::XRPC.nsid,
            ForgetDrawer::XRPC.method,
            &serde_json::to_value(ForgetDrawer {
                capability,
                drawer_id,
            })?,
        )
        .await?;
    eyre::ensure!(
        reply.is_success(),
        "forgetting the drawer failed with {}: {}",
        reply.status,
        reply.body
    );
    println!("forgot {drawer}");
    Ok(ExitCode::SUCCESS)
}

/// Learns the known-drawers drawer's id from `getNodeInfo` and mints an admin handle on just that
/// drawer, which is what listing, remembering and forgetting authorize against, for the one call
/// that follows.
async fn known_drawers_handle(
    client: &crate::client::NodeClient,
) -> Res<big_cabinet_core::authority::CapId> {
    let info = super::status::node_info(client).await?;
    let minted = client
        .call(
            MintDrawerCapability::XRPC.nsid,
            MintDrawerCapability::XRPC.method,
            &serde_json::to_value(MintDrawerCapability {
                scope: DrawerScope {
                    drawers: [(info.known_drawers_id, DrawerAccess::Admin)].into(),
                    create: false,
                },
                rights: Rights::USE,
                subject: None,
            })?,
        )
        .await?;
    eyre::ensure!(
        minted.is_success(),
        "minting a handle on the known-drawers list failed with {}: {}",
        minted.status,
        minted.body
    );
    serde_json::from_value(minted.body["capabilityId"].clone())
        .wrap_err("the node's answer carries no capability id")
}

fn parse_drawer(drawer: &str) -> Res<DocId> {
    drawer
        .parse()
        .map_err(|error| eyre::eyre!("`{drawer}` is not a drawer id: {error}"))
}

/// Parses the drawer and peer arguments, and mints an admin handle on the drawer for the one
/// call that follows.
async fn admin_handle(
    client: &crate::client::NodeClient,
    drawer: &str,
    peer: &str,
) -> Res<(DocId, GroupKey, big_cabinet_core::authority::CapId)> {
    let drawer_id = parse_drawer(drawer)?;
    let peer_bytes: [u8; 32] = data_encoding::HEXLOWER
        .decode(peer.as_bytes())
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| {
            eyre::eyre!(
                "`{peer}` is not a node id (64 lowercase hex digits, as `day node status` prints)"
            )
        })?;
    let minted = client
        .call(
            MintDrawerCapability::XRPC.nsid,
            MintDrawerCapability::XRPC.method,
            &serde_json::to_value(MintDrawerCapability {
                scope: DrawerScope {
                    drawers: [(drawer_id.clone(), DrawerAccess::Admin)].into(),
                    create: false,
                },
                rights: Rights::USE,
                subject: None,
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
    Ok((drawer_id, GroupKey(peer_bytes), capability))
}

/// Adds the peer as a member of the drawer. The node refuses a peer it has not met, which this
/// says in the terms of the commands that fix it.
async fn add_member(drawer: &str, peer: &str, access: DrawerAccess) -> Res<ExitCode> {
    let client = lazy::node().await?;
    let (drawer_id, peer_id, capability) = admin_handle(&client, drawer, peer).await?;
    let reply = client
        .call(
            InvitePrincipal::XRPC.nsid,
            InvitePrincipal::XRPC.method,
            &serde_json::to_value(InvitePrincipal {
                capability,
                drawer_id,
                principal: Principal::Peer { peer_id },
                access,
            })?,
        )
        .await?;
    if reply.is_success() {
        println!("added {peer} to {drawer} at {access:?}");
        return Ok(ExitCode::SUCCESS);
    }
    if reply.body["error"] == "UnknownPeer" {
        error!(
            "this node has not met {peer}: connect to it first (`day peers add <ticket>` and a daemon restart), then retry"
        );
    } else {
        error!(
            status = reply.status,
            "the node refused the member: {}: {}",
            reply.body["error"].as_str().unwrap_or("unnamed error"),
            reply.body["message"].as_str().unwrap_or_default()
        );
    }
    Ok(ExitCode::FAILURE)
}

/// Removes the peer's membership of the drawer.
async fn revoke_member(drawer: &str, peer: &str) -> Res<ExitCode> {
    let client = lazy::node().await?;
    let (drawer_id, peer_id, capability) = admin_handle(&client, drawer, peer).await?;
    let reply = client
        .call(
            RevokePrincipal::XRPC.nsid,
            RevokePrincipal::XRPC.method,
            &serde_json::to_value(RevokePrincipal {
                capability,
                drawer_id,
                principal: Principal::Peer { peer_id },
            })?,
        )
        .await?;
    if reply.is_success() {
        println!("revoked {peer} from {drawer}");
        return Ok(ExitCode::SUCCESS);
    }
    error!(
        status = reply.status,
        "the node refused the revocation: {}: {}",
        reply.body["error"].as_str().unwrap_or("unnamed error"),
        reply.body["message"].as_str().unwrap_or_default()
    );
    Ok(ExitCode::FAILURE)
}
