use std::path::PathBuf;
use std::process::ExitCode;
use utils_rs::prelude::*;
use wire_surfaces::snapshot::Snapshot;

#[derive(clap::Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
    /// The snapshot directory; defaults to this crate's `snapshot/`.
    #[arg(long, global = true)]
    dir: Option<PathBuf>,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Fail when the checked-in snapshot is stale or a Lexicon evolved against the rules.
    Check,
    /// Rewrite the snapshot; refused when a Lexicon evolved against the rules.
    Bless,
}

fn main() -> Res<ExitCode> {
    use clap::Parser;
    let args = Args::parse();
    let dir = args
        .dir
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("snapshot"));
    let snapshot = Snapshot::generate()?;
    let problems = snapshot.problems(&dir)?;
    match args.command {
        Command::Check if problems.is_empty() => {
            println!("wire: {dir:?} ok ({})", snapshot.summary());
            Ok(ExitCode::SUCCESS)
        }
        Command::Bless if problems.evolution.is_empty() => {
            for line in snapshot.stabilized(&dir)? {
                println!("{line}");
            }
            snapshot.bless(&dir)?;
            println!("wire: wrote {dir:?} ({})", snapshot.summary());
            Ok(ExitCode::SUCCESS)
        }
        Command::Check | Command::Bless => {
            for line in problems.render() {
                eprintln!("ERROR_WIRE_SURFACE {line}");
            }
            Ok(ExitCode::FAILURE)
        }
    }
}
