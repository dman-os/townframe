use crate::client::CallError;
use crate::interlude::*;

use big_cabinet_core::DocId;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::GroupKey;
use big_cabinet_core::drawer::listing::KnownDrawers;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::DrawersError;
use big_cabinet_core::drawer::protocol::ForgetDrawer;
use big_cabinet_core::drawer::protocol::InvitePrincipal;
use big_cabinet_core::drawer::protocol::ListKnownDrawers;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::drawer::protocol::Principal;
use big_cabinet_core::drawer::protocol::RememberDrawer;
use big_cabinet_core::drawer::protocol::RevokePrincipal;

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

    let known: KnownDrawers = client
        .call(ListKnownDrawers { capability })
        .await
        .wrap_err("listing the known drawers failed")?;
    if as_json {
        println!("{}", serde_json::to_string_pretty(&known)?);
        return Ok(ExitCode::SUCCESS);
    }
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
    client
        .call(RememberDrawer {
            capability,
            drawer_id,
            mirror,
        })
        .await
        .wrap_err("remembering the drawer failed")?;
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
    client
        .call(ForgetDrawer {
            capability,
            drawer_id,
        })
        .await
        .wrap_err("forgetting the drawer failed")?;
    println!("forgot {drawer}");
    Ok(ExitCode::SUCCESS)
}

/// Learns the known-drawers drawer's id from `getNodeInfo` and mints an admin handle on just that
/// drawer, which is what listing, remembering and forgetting authorize against, for the one call
/// that follows.
pub(super) async fn known_drawers_handle(
    client: &crate::client::NodeClient,
) -> Res<big_cabinet_core::authority::CapId> {
    let info = super::status::node_info(client).await?;
    let minted = client
        .call(MintDrawerCapability {
            scope: DrawerScope {
                drawers: [(info.known_drawers_id, DrawerAccess::Admin)].into(),
                create: false,
            },
            rights: Rights::USE,
            subject: None,
        })
        .await
        .wrap_err("minting a handle on the known-drawers list failed")?;
    Ok(minted.capability_id)
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
        .call(MintDrawerCapability {
            scope: DrawerScope {
                drawers: [(drawer_id.clone(), DrawerAccess::Admin)].into(),
                create: false,
            },
            rights: Rights::USE,
            subject: None,
        })
        .await
        .wrap_err("minting a handle on the drawer failed")?;
    Ok((drawer_id, GroupKey(peer_bytes), minted.capability_id))
}

/// Adds the peer as a member of the drawer. The node refuses a peer it has not met, which this
/// says in the terms of the commands that fix it.
async fn add_member(drawer: &str, peer: &str, access: DrawerAccess) -> Res<ExitCode> {
    let client = lazy::node().await?;
    let (drawer_id, peer_id, capability) = admin_handle(&client, drawer, peer).await?;
    let invited = client
        .call(InvitePrincipal {
            capability,
            drawer_id,
            principal: Principal::Peer { peer_id },
            access,
        })
        .await;
    match invited {
        Ok(()) => {
            println!("added {peer} to {drawer} at {access:?}");
            Ok(ExitCode::SUCCESS)
        }
        Err(CallError::Transport(error)) => Err(error),
        Err(CallError::Method {
            error: DrawersError::UnknownPeer(_),
            ..
        }) => {
            error!(
                "this node has not met {peer}: connect to it first (`day peers add <ticket>` and a server restart), then retry"
            );
            Ok(ExitCode::FAILURE)
        }
        Err(refused) => {
            error!(
                status = refused.status(),
                "the node refused the member: {refused}"
            );
            Ok(ExitCode::FAILURE)
        }
    }
}

/// Removes the peer's membership of the drawer.
async fn revoke_member(drawer: &str, peer: &str) -> Res<ExitCode> {
    let client = lazy::node().await?;
    let (drawer_id, peer_id, capability) = admin_handle(&client, drawer, peer).await?;
    let revoked = client
        .call(RevokePrincipal {
            capability,
            drawer_id,
            principal: Principal::Peer { peer_id },
        })
        .await;
    match revoked {
        Ok(()) => {
            println!("revoked {peer} from {drawer}");
            Ok(ExitCode::SUCCESS)
        }
        Err(CallError::Transport(error)) => Err(error),
        Err(refused) => {
            error!(
                status = refused.status(),
                "the node refused the revocation: {refused}"
            );
            Ok(ExitCode::FAILURE)
        }
    }
}
