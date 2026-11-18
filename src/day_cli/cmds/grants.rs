use crate::client::Mode;
use crate::client::NodeClient;
use crate::interlude::*;

use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::drawer::DrawerAccess;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::config::GrantBody;
use big_cartridges_core::config::GrantKey;
use big_cartridges_core::protocol::AnswerCartridgeGrantRequest;
use big_cartridges_core::protocol::CartridgeGrantRecord;
use big_cartridges_core::protocol::CartridgeGrants;
use big_cartridges_core::protocol::GrantAnswer;
use big_cartridges_core::protocol::GrantRequestRecord;
use big_cartridges_core::protocol::GrantRequests;
use big_cartridges_core::protocol::ListCartridgeGrantRequests;
use big_cartridges_core::protocol::ListCartridgeGrants;
use big_cartridges_core::protocol::RequestedAccess;
use big_cartridges_core::protocol::SetCartridgeGrant;
use method_core::Method;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// What cartridges may reach, and the requests they have made for more (FDR 009).
///
/// A cartridge asks while it runs; the request waits here, and on the shell, until a person
/// answers it once. "Only this time" and "don't allow" last until the node is next started.
#[derive(Debug, clap::Subcommand)]
pub enum GrantsCommands {
    /// List the requests waiting for an answer
    ///
    /// One per line: its id, the cartridge, what it asks and when. Requests live in the running
    /// node's memory, so this needs the node running (`day node run`).
    Pending {
        /// Print the node's raw answer as JSON
        #[arg(long)]
        json: bool,
    },
    /// Say yes to a request
    ///
    /// Without `--once` the answer is kept, follows you to your other devices, and can be taken
    /// back with `day grants revoke`.
    Accept {
        /// The request's id, as `day grants pending` prints it
        request: String,
        /// Only this time: until the node is next started, and written nowhere
        #[arg(long)]
        once: bool,
        /// For a request about a drawer that names none: the drawer to allow
        #[arg(long, value_name = "DRAWER")]
        drawer: Option<String>,
    },
    /// Say no to a request
    ///
    /// The cartridge is not asked about the same thing again until the node is next started.
    Decline {
        /// The request's id, as `day grants pending` prints it
        request: String,
    },
    /// List what cartridges may do, all of them or one cartridge's
    List {
        /// Only this cartridge's answers, as `<did>/<name>`
        cartridge: Option<String>,
        /// Print the node's raw answer as JSON
        #[arg(long)]
        json: bool,
    },
    /// Take back one answer
    ///
    /// Takes effect on the cartridge's next action.
    Revoke {
        /// The cartridge, as `<did>/<name>`
        cartridge: String,
        /// The answer to take back, as `day grants list` prints it: `drawer:<id>`,
        /// `cartridge:<id>`, `net:<host[:port]>`, `method:<nsid>` or `authority:<prefix>`
        grant: String,
    },
}

pub async fn run(command: GrantsCommands) -> Res<ExitCode> {
    let client = lazy::node().await?;
    match command {
        GrantsCommands::Pending { json } => pending(&client, json).await,
        GrantsCommands::Accept {
            request,
            once,
            drawer,
        } => {
            let answer = if once {
                GrantAnswer::OnlyThisBoot
            } else {
                GrantAnswer::Allow
            };
            answer_request(&client, &request, answer, drawer).await
        }
        GrantsCommands::Decline { request } => {
            answer_request(&client, &request, GrantAnswer::Decline, None).await
        }
        GrantsCommands::List { cartridge, json } => list(&client, cartridge, json).await,
        GrantsCommands::Revoke { cartridge, grant } => revoke(&client, &cartridge, &grant).await,
    }
}

/// Requests exist only in the memory of the node that is running, so a command that answers or
/// lists them is refused with the way to a running node rather than shown an empty queue.
fn require_running_node(client: &NodeClient) -> Res<()> {
    eyre::ensure!(
        client.mode() == Mode::Daemon,
        "no node is running here, and grant requests live in a running node's memory: start it \
         with `day node run`"
    );
    Ok(())
}

async fn pending_requests(client: &NodeClient) -> Res<(serde_json::Value, GrantRequests)> {
    require_running_node(client)?;
    let reply = client
        .call(
            ListCartridgeGrantRequests::XRPC.nsid,
            ListCartridgeGrantRequests::XRPC.method,
            &json!({}),
        )
        .await?;
    eyre::ensure!(
        reply.is_success(),
        "listing the requests failed with {}: {}",
        reply.status,
        reply.body
    );
    let requests = serde_json::from_value(reply.body.clone())?;
    Ok((reply.body, requests))
}

async fn pending(client: &NodeClient, as_json: bool) -> Res<ExitCode> {
    let (raw, requests) = pending_requests(client).await?;
    if as_json {
        println!("{}", serde_json::to_string_pretty(&raw)?);
        return Ok(ExitCode::SUCCESS);
    }
    let now = now_ms();
    for request in requests.requests {
        println!(
            "{}  {}  {}  {}",
            request.id,
            request.cartridge_id,
            question(&request.access),
            age(now.saturating_sub(request.filed_at_ms)),
        );
        println!("    {}", request.description);
    }
    Ok(ExitCode::SUCCESS)
}

/// What a request asks, in a person's words and never the manifest's binding: the shell words
/// the same question the same way.
fn question(access: &RequestedAccess) -> String {
    match access {
        RequestedAccess::Drawers { access, drawer } => {
            let verb = match access {
                DrawerAccess::Read => "read",
                _ => "change",
            };
            match drawer {
                Some(drawer) => format!("may {verb} your drawer {drawer}"),
                None => format!("may {verb} a drawer you pick"),
            }
        }
        RequestedAccess::Method { nsid } => format!("may use the operation `{nsid}`"),
        RequestedAccess::Net { host, port } => match port {
            Some(port) => format!("may use the network to reach {host}:{port}"),
            None => format!("may use the network to reach {host}"),
        },
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is past the Unix epoch")
            .as_millis(),
    )
    .expect("milliseconds since the epoch fit in a u64")
}

fn age(elapsed_ms: u64) -> String {
    let seconds = elapsed_ms / 1000;
    match seconds {
        0..60 => "just now".to_owned(),
        60..3600 => format!("{}m ago", seconds / 60),
        3600..86_400 => format!("{}h ago", seconds / 3600),
        _ => format!("{}d ago", seconds / 86_400),
    }
}

async fn answer_request(
    client: &NodeClient,
    request: &str,
    answer: GrantAnswer,
    drawer: Option<String>,
) -> Res<ExitCode> {
    let id: Uuid7 = serde_json::from_value(request.into())
        .map_err(|error| eyre::eyre!("`{request}` is not a request id: {error}"))?;
    let drawer = drawer
        .map(|drawer| {
            drawer
                .parse()
                .map_err(|error| eyre::eyre!("`{drawer}` is not a drawer id: {error}"))
        })
        .transpose()?;
    let (_, requests) = pending_requests(client).await?;
    let Some(asked) = requests.requests.iter().find(|asked| asked.id == id) else {
        error!("there is no waiting request {id}: `day grants pending` lists them");
        return Ok(ExitCode::FAILURE);
    };
    let reply = client
        .call(
            AnswerCartridgeGrantRequest::XRPC.nsid,
            AnswerCartridgeGrantRequest::XRPC.method,
            &serde_json::to_value(AnswerCartridgeGrantRequest {
                request: id,
                answer,
                drawer: drawer.clone(),
            })?,
        )
        .await?;
    if !reply.is_success() {
        error!(
            status = reply.status,
            "the node refused the answer: {}: {}",
            reply.body["error"].as_str().unwrap_or("unnamed error"),
            reply.body["message"].as_str().unwrap_or_default()
        );
        return Ok(ExitCode::FAILURE);
    }
    let GrantRequestRecord {
        cartridge_id,
        access,
        ..
    } = asked;
    let access = match (access, drawer) {
        (RequestedAccess::Drawers { access, .. }, Some(picked)) => RequestedAccess::Drawers {
            access: *access,
            drawer: Some(picked),
        },
        (access, _) => access.clone(),
    };
    let question = question(&access);
    match answer {
        GrantAnswer::Allow => println!("Allowed: {cartridge_id} {question}."),
        GrantAnswer::OnlyThisBoot => println!(
            "Allowed only this time: {cartridge_id} {question}, until the node is next started."
        ),
        GrantAnswer::Decline => println!(
            "Declined: {cartridge_id} will not be asked about this again until the node is next \
             started."
        ),
    }
    Ok(ExitCode::SUCCESS)
}

/// What a grant lets a cartridge do, in a person's words.
fn grant_sentence(grant: &GrantKey, body: &GrantBody) -> String {
    let access = |body: &GrantBody| match body.access.as_ref().and_then(|access| access.known()) {
        Some(DrawerAccess::Relay) => "relay",
        Some(DrawerAccess::Read) => "read",
        Some(DrawerAccess::Write) => "write",
        Some(DrawerAccess::Admin) => "administer",
        None => "use",
    };
    match grant {
        GrantKey::Drawer(drawer) => format!("may {} your drawer {drawer}", access(body)),
        GrantKey::Cartridge(cartridge) => {
            format!("may {} your data from {cartridge}", access(body))
        }
        GrantKey::Net(net) => format!("may use the network to reach {net}"),
        GrantKey::Method(nsid) => format!("may use the operation `{nsid}`"),
        GrantKey::Authority(prefix) if prefix == "*" => "may use any operation".to_owned(),
        GrantKey::Authority(prefix) => format!("may use the operations under `{prefix}`"),
    }
}

async fn list(client: &NodeClient, cartridge: Option<String>, as_json: bool) -> Res<ExitCode> {
    let cartridge_id: Option<CartridgeId> = cartridge
        .map(|cartridge| {
            cartridge
                .parse()
                .map_err(|error| eyre::eyre!("`{cartridge}` is not a cartridge id: {error}"))
        })
        .transpose()?;
    let reply = client
        .call(
            ListCartridgeGrants::XRPC.nsid,
            ListCartridgeGrants::XRPC.method,
            &serde_json::to_value(ListCartridgeGrants { cartridge_id })?,
        )
        .await?;
    eyre::ensure!(
        reply.is_success(),
        "listing the grants failed with {}: {}",
        reply.status,
        reply.body
    );
    if as_json {
        println!("{}", serde_json::to_string_pretty(&reply.body)?);
        return Ok(ExitCode::SUCCESS);
    }
    let CartridgeGrants { grants } = serde_json::from_value(reply.body)?;
    for CartridgeGrantRecord {
        cartridge_id,
        grant,
        value,
        this_boot,
    } in grants
    {
        println!(
            "{cartridge_id}  {grant}  {}{}",
            grant_sentence(&grant, &value),
            if this_boot { " (only this time)" } else { "" },
        );
    }
    Ok(ExitCode::SUCCESS)
}

async fn revoke(client: &NodeClient, cartridge: &str, grant: &str) -> Res<ExitCode> {
    let cartridge_id: CartridgeId = cartridge
        .parse()
        .map_err(|error| eyre::eyre!("`{cartridge}` is not a cartridge id: {error}"))?;
    let grant: GrantKey = grant.parse()?;
    let reply = client
        .call(
            SetCartridgeGrant::XRPC.nsid,
            SetCartridgeGrant::XRPC.method,
            &serde_json::to_value(SetCartridgeGrant {
                cartridge_id,
                grant: grant.clone(),
                value: None,
            })?,
        )
        .await?;
    if !reply.is_success() {
        println!("{}", serde_json::to_string_pretty(&reply.body)?);
        error!(
            status = reply.status,
            "the node refused to take the answer back"
        );
        return Ok(ExitCode::FAILURE);
    }
    println!("Took back `{grant}` from {cartridge}. It takes effect on its next action.");
    Ok(ExitCode::SUCCESS)
}
