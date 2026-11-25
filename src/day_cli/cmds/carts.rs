use crate::client::CallError;
use crate::interlude::*;

use big_cartridges_core::CartridgeId;
use big_nodule_core::protocol::SetNoduleDevTakeover;

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
    let taken = lazy::node()
        .await?
        .call(SetNoduleDevTakeover {
            cartridge_id: parse_cartridge(slot)?,
            by: by.as_deref().map(parse_cartridge).transpose()?,
        })
        .await;
    match taken {
        Ok(_) => {}
        Err(CallError::Transport(error)) => return Err(error),
        Err(refused) => {
            error!(
                status = refused.status(),
                "the node refused the takeover: {refused}"
            );
            return Ok(ExitCode::FAILURE);
        }
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

fn parse_cartridge(cartridge: &str) -> Res<CartridgeId> {
    cartridge
        .parse()
        .map_err(|error| eyre::eyre!("`{cartridge}` is not a cartridge id: {error}"))
}
