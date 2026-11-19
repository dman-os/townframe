//! The `day` CLI's reference documentation, rendered from its clap definitions by the hidden
//! `day docs` command, which only a build with the `docs` feature of `day_cli` has. The Markdown
//! reference is checked in and checked like the wire snapshot (`check`, or `--bless` to rewrite
//! it); the man pages are build output.

use crate::interlude::*;

/// Where the checked-in Markdown reference lives, relative to the workspace root.
const REFERENCE: &str = "docs/cli/day.md";

#[derive(Debug, clap::Subcommand)]
pub enum CliCmd {
    /// Fail when the checked-in Markdown reference (docs/cli/day.md) is not what the CLI's clap
    /// definitions render; with `--bless`, rewrite it.
    Docs {
        #[arg(long)]
        bless: bool,
    },
    /// Write the man pages, one per command, into a directory.
    Man {
        /// Defaults to `target/man` under the workspace root.
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

pub fn dispatch(root: &Path, cmd: CliCmd) -> Res<()> {
    match cmd {
        CliCmd::Docs { bless } => {
            let rendered = day_docs(root, &["markdown"])?;
            let path = root.join(REFERENCE);
            if bless {
                std::fs::create_dir_all(path.parent().expect("the reference sits in a directory"))?;
                std::fs::write(&path, &rendered)?;
                println!("cli: wrote {REFERENCE}");
                return Ok(());
            }
            let checked_in = std::fs::read(&path).unwrap_or_default();
            eyre::ensure!(
                checked_in == rendered,
                "{REFERENCE} is stale: run `cargo x cli docs --bless` and commit it"
            );
            println!("cli: {REFERENCE} ok");
            Ok(())
        }
        CliCmd::Man { out } => {
            let out = out.unwrap_or_else(|| root.join("target/man"));
            day_docs(root, &["man", &out.to_string_lossy()])?;
            println!("cli: man pages in {}", out.display());
            Ok(())
        }
    }
}

/// Runs `day docs <args>` from a build of `day_cli` with the `docs` feature and returns what it
/// printed.
fn day_docs(root: &Path, args: &[&str]) -> Res<Vec<u8>> {
    let output = std::process::Command::new("cargo")
        .current_dir(root)
        .args([
            "run",
            "--quiet",
            "-p",
            "day_cli",
            "--features",
            "docs",
            "--bin",
            "day",
        ])
        .args(["--", "docs"])
        .args(args)
        .stderr(std::process::Stdio::inherit())
        .output()?;
    eyre::ensure!(output.status.success(), "`day docs` failed");
    Ok(output.stdout)
}
