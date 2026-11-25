//! `day docs`: the CLI's reference documentation, rendered from its clap definitions. It is
//! compiled only with the `docs` feature and hidden from the help, since it is tooling for
//! `cargo x cli`, which keeps the checked-in Markdown reference current and builds the man pages.

use crate::interlude::*;
use crate::out::out;

#[derive(Debug, clap::Subcommand)]
pub enum DocsCommands {
    /// Print the Markdown CLI reference
    Markdown,
    /// Write one man page per command into a directory
    Man {
        /// The directory to write `day.1`, `day-node.1` and the rest into
        dir: PathBuf,
    },
}

pub fn run(command: DocsCommands) -> Res<ExitCode> {
    use clap::CommandFactory;
    let cli = crate::Cli::command();
    match command {
        DocsCommands::Markdown => {
            let options = clap_markdown::MarkdownOptions::new().show_footer(false);
            let reference = clap_markdown::help_markdown_command_custom(&cli, &options);
            out().write_all(reference.as_bytes())?;
        }
        DocsCommands::Man { dir } => {
            std::fs::create_dir_all(&dir)?;
            clap_mangen::generate_to(cli, &dir)?;
        }
    }
    Ok(ExitCode::SUCCESS)
}
