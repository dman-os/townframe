use crate::interlude::*;

use big_nodule_core::protocol::SetNoduleDevTakeover;
use method_core::Method;

/// Cartridges installed on the node.
#[derive(Debug, clap::Subcommand)]
pub enum CartsCommands {
    /// Dev hook: run an installed dev build AS another installed cartridge, to test it against
    /// real data
    ///
    /// The dev build takes over the slot: it runs under the original's actor with its data, its
    /// drawer grants in full and the method grants it asks for that the original's own code
    /// asked for too. Nothing else does this: routing a method to another provider
    /// (`org.example.daybook.nodules.setMethodRoute`) leaves each provider its own data and
    /// grants. The node refuses unless its node.json sets `dev.allow_slot_takeover`, and the
    /// dev build must not be enabled on its own.
    DevTakeover {
        /// The cartridge whose slot is taken over, as `<did>/<name>`
        slot: String,
        /// The installed cartridge that runs as the slot
        #[arg(long, required_unless_present = "clear", conflicts_with = "clear")]
        by: Option<String>,
        /// End the takeover: the slot runs its own install again
        #[arg(long)]
        clear: bool,
    },
}

pub async fn run(command: CartsCommands) -> Res<ExitCode> {
    match command {
        CartsCommands::DevTakeover { slot, by, clear } => {
            debug_assert_eq!(
                by.is_none(),
                clear,
                "clap requires exactly one of --by, --clear"
            );
            dev_takeover(&slot, by).await
        }
    }
}

async fn dev_takeover(slot: &str, by: Option<String>) -> Res<ExitCode> {
    let reply = lazy::node()
        .await?
        .call(
            SetNoduleDevTakeover::XRPC.nsid,
            SetNoduleDevTakeover::XRPC.method,
            &json!({ "cartridgeId": slot, "by": by }),
        )
        .await?;
    if !reply.is_success() {
        println!("{}", serde_json::to_string_pretty(&reply.body)?);
        error!(status = reply.status, "the node refused the takeover");
        return Ok(ExitCode::FAILURE);
    }
    match by {
        Some(by) => println!(
            "`{by}` now runs as `{slot}`, with its actor, its data, every drawer grant and the \
             method grants both asked for. `day carts dev-takeover {slot} --clear` ends it."
        ),
        None => println!("`{slot}` runs its own install again"),
    }
    Ok(ExitCode::SUCCESS)
}
