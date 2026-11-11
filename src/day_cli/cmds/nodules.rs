use crate::context::SocketState;
use crate::interlude::*;
use std::collections::BTreeSet;

use big_dispatch_core::DispatchScope;

#[derive(Debug, clap::Subcommand)]
pub enum NodulesCommands {
    /// Manage the operation ceiling: the most any nodule can be granted to invoke
    Ceiling {
        #[clap(subcommand)]
        command: CeilingCommands,
    },
}

#[derive(Debug, clap::Subcommand)]
pub enum CeilingCommands {
    /// Allow nodules to be granted these operations
    ///
    /// The ceiling starts empty, which lets nodules invoke nothing: a grant outside it is
    /// reported on the nodule's record as not applied. Granting is still per nodule; this only
    /// raises what a grant may reach.
    Add(CeilingEntries),
    /// Take entries back out of the ceiling
    Rm(CeilingEntries),
    /// Print the ceiling
    Ls,
}

#[derive(Debug, clap::Args)]
pub struct CeilingEntries {
    /// An authority: an NSID prefix covering every operation at or under it, on whole segments
    /// (`org.example.photos` covers `org.example.photos.list`, not `org.example.photosynthesis`)
    #[arg(long = "authority", value_name = "NSID")]
    authorities: Vec<String>,
    /// One exact operation NSID
    #[arg(long = "operation", value_name = "NSID")]
    operations: Vec<String>,
}

impl CeilingEntries {
    /// Refuses an empty request and anything that is not an NSID, before a write is attempted.
    fn validate(&self) -> Res<()> {
        eyre::ensure!(
            !self.authorities.is_empty() || !self.operations.is_empty(),
            "name at least one --authority or --operation"
        );
        for nsid in self.authorities.iter().chain(&self.operations) {
            crate::client::ensure_nsid(nsid)?;
        }
        Ok(())
    }
}

pub async fn run(command: NodulesCommands) -> Res<ExitCode> {
    match command {
        NodulesCommands::Ceiling { command } => match command {
            CeilingCommands::Add(entries) => edit(&entries, true).await,
            CeilingCommands::Rm(entries) => edit(&entries, false).await,
            CeilingCommands::Ls => ls().await,
        },
    }
}

/// Inserts (`add`) or removes each of `nsids`, reporting per NSID whether the set changed.
fn apply<'a>(set: &mut BTreeSet<String>, nsids: &'a [String], add: bool) -> Vec<(&'a str, bool)> {
    nsids
        .iter()
        .map(|nsid| {
            let changed = if add {
                set.insert(nsid.clone())
            } else {
                set.remove(nsid)
            };
            (nsid.as_str(), changed)
        })
        .collect()
}

/// Adds or removes the entries in `node.json`'s `nodule_operations`, printing one line per entry
/// and whether it changed anything. The settings are read once, when a daemon boots, so this
/// never reaches into a running node: a serving daemon is told that it has to restart for the
/// new ceiling to apply.
async fn edit(entries: &CeilingEntries, add: bool) -> Res<ExitCode> {
    entries.validate()?;
    let conf = lazy::config().await?;
    let dir = &conf.node_dir;
    let (authorities, operations) = dir.update_settings(|settings| {
        let scope = &mut settings.nodule_operations;
        Ok((
            apply(&mut scope.authorities, &entries.authorities, add),
            apply(&mut scope.operations, &entries.operations, add),
        ))
    })?;
    let mut any_changed = false;
    for (kind, changes) in [("authority", authorities), ("operation", operations)] {
        for (nsid, changed) in changes {
            any_changed |= changed;
            println!(
                "{kind} {nsid}: {}",
                match (add, changed) {
                    (true, true) => "added to the ceiling",
                    (true, false) => "already in the ceiling",
                    (false, true) => "removed from the ceiling",
                    (false, false) => "not in the ceiling",
                }
            );
        }
    }
    if any_changed && dir.probe_socket().await? == SocketState::Live {
        println!("the running daemon applies it after `day daemon stop` and `day daemon start`");
    }
    Ok(ExitCode::SUCCESS)
}

/// Prints `node.json`'s ceiling: what the next daemon start will use.
async fn ls() -> Res<ExitCode> {
    let conf = lazy::config().await?;
    let DispatchScope {
        operations,
        authorities,
    } = conf.node_dir.settings()?.nodule_operations;
    println!("authorities {}", authorities.len());
    for nsid in &authorities {
        println!("  {nsid}");
    }
    println!("operations {}", operations.len());
    for nsid in &operations {
        println!("  {nsid}");
    }
    Ok(ExitCode::SUCCESS)
}
