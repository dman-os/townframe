#[expect(unused)]
mod interlude {
    pub use std::future::Future;
    pub use std::path::Path;
    pub use std::path::PathBuf;
    pub use std::sync::Arc;

    pub use utils_rs::prelude::*;
}

mod guardrails;
mod plug;
mod sql;

use clap::builder::styling::AnsiColor;

use crate::interlude::*;

fn main() -> Res<()> {
    dotenv_flow::dotenv_flow().ok();
    utils_rs::setup_tracing()?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(main_main())
}

async fn main_main() -> Res<()> {
    use clap::Parser;
    match Args::parse().command {
        Commands::Sql { command } => sql::dispatch(&workspace_root().await?, command).await,
        Commands::Plug { command } => plug::dispatch(command).await,
        Commands::Guardrails { command } => guardrails::dispatch(&workspace_root().await?, command),
    }
}

/// Root of the Cargo workspace xtask belongs to, independent of the cwd.
pub async fn workspace_root() -> Res<PathBuf> {
    let output = tokio::process::Command::new("cargo")
        .args(["locate-project", "--workspace", "--message-format", "plain"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .await?;
    eyre::ensure!(
        output.status.success(),
        "cargo locate-project failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let manifest = PathBuf::from(String::from_utf8(output.stdout)?.trim());
    Ok(manifest
        .parent()
        .ok_or_eyre("workspace manifest has no parent")?
        .to_path_buf())
}

const CLAP_STYLE: clap::builder::Styles = clap::builder::Styles::styled()
    .header(AnsiColor::Yellow.on_default())
    .usage(AnsiColor::Green.on_default())
    .literal(AnsiColor::Green.on_default())
    .placeholder(AnsiColor::Green.on_default());

#[derive(Debug, clap::Parser)]
#[clap(
    version,
    about,
    styles = CLAP_STYLE
)]
struct Args {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, clap::Subcommand)]
enum Commands {
    /// SQLite units: validate, generate `sqlite/gen.rs`, and apply migrations.
    Sql {
        #[command(subcommand)]
        command: sql::SqlCmd,
    },
    /// Plug packaging.
    Plug {
        #[command(subcommand)]
        command: plug::PlugCmd,
    },
    /// Source guardrails: lint, API snapshots, build contracts, and validation receipts.
    Guardrails {
        #[command(subcommand)]
        command: guardrails::GuardrailsCmd,
    },
}
