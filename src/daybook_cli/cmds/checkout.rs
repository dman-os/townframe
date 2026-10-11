use crate::interlude::*;

#[derive(Debug, clap::Subcommand)]
pub enum DtreeCommands {
    /// Create a single-document checkout from one text/plain Body Note and dpath
    Create {
        directory: PathBuf,
        #[arg(long)]
        doc: String,
    },
    /// Ingest local file edits as document operations staged on the
    /// checkout-local branch. Never publishes: upstream stays untouched.
    Ingest {
        directory: Option<PathBuf>,
        /// Explicitly import an untracked file as a new document (repeatable;
        /// only the raw-text Note representation is supported)
        #[arg(long, value_name = "PATH")]
        allow: Vec<PathBuf>,
        /// Resolve a claim left by an interrupted import: adopt the document
        /// that was already created for this path on main
        /// (repeatable; PATH=DOC)
        #[arg(long, value_name = "PATH=DOC")]
        adopt_import_claim: Vec<String>,
        /// Resolve a claim left by an interrupted import by dropping it
        /// (repeatable; the file returns to untracked, any created document
        /// stays unbound and remains the user's business)
        #[arg(long, value_name = "PATH")]
        drop_import_claim: Vec<String>,
    },
    /// Report local files without ingesting or publishing (nearest checkout by default)
    Status { directory: Option<PathBuf> },
    /// Publish staged, receipt-settled work upstream. Each destination
    /// recomputes the validated candidate merge at fresh upstream heads and
    /// publishes through the expected-heads CAS; refuses unsettled staging
    /// instead of publishing implicitly (ADR 011 §5).
    Publish { directory: Option<PathBuf> },
}

pub async fn run(command: DtreeCommands) -> Res<ExitCode> {
    #[cfg(unix)]
    {
        unix::run(command).await
    }
    #[cfg(not(unix))]
    {
        let _ = command;
        eyre::bail!("checkout filesystem delivery currently requires Unix");
    }
}

#[cfg(unix)]
mod unix;
