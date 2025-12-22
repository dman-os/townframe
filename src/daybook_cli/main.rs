#[allow(unused)]
mod interlude {
    pub use std::path::PathBuf;
    pub use std::sync::Arc;

    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

mod context;

use clap::builder::styling::AnsiColor;

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
    let args = Args::parse();
    match args.command {
        Commands::Docs { id } => {
            let ctx = context::init_context().await?;
            let drawer_doc_id = ctx.doc_drawer().document_id().clone();
            let repo = daybook_core::drawer::DrawerRepo::load(ctx.acx.clone(), drawer_doc_id)
                .await?;

            if let Some(id) = id {
                // Show details for a specific document
                let doc = repo.get(&id).await?;
                if let Some(doc) = doc {
                    println!("{:#?}", doc);
                } else {
                    eyre::bail!("Document not found: {}", id);
                }
            } else {
                // List all documents
                let doc_ids = repo.list().await;
                let mut docs = Vec::new();
                for doc_id in &doc_ids {
                    if let Some(doc) = repo.get(doc_id).await? {
                        docs.push((doc_id.clone(), doc));
                    }
                }

                // Display in table format using comfy-table (kubectl-style, no borders)
                use comfy_table::presets::NOTHING;
                use comfy_table::Table;
                let mut table = Table::new();
                table
                    .load_preset(NOTHING)
                    .set_header(vec!["ID", "Title"]);
                for (id, doc) in docs {
                    let title = doc
                        .props
                        .iter()
                        .find_map(|prop| match prop {
                            daybook_types::DocProp::TitleGeneric(s) => Some(s.clone()),
                            _ => None,
                        })
                        .unwrap_or_else(|| "<no title>".to_string());
                    table.add_row(vec![id, title]);
                }
                println!("{table}");
            }
        }
        Commands::Completions { shell } => {
            use clap::CommandFactory;
            let mut cmd = Args::command();
            clap_complete::generate(shell, &mut cmd, "daybook", &mut std::io::stdout());
        }
    }

    Ok(())
}

const CLAP_STYLE: clap::builder::Styles = clap::builder::Styles::styled()
    .header(AnsiColor::Yellow.on_default())
    .usage(AnsiColor::Green.on_default())
    .literal(AnsiColor::Green.on_default())
    .placeholder(AnsiColor::Green.on_default());

#[derive(Debug, clap::Parser)]
#[clap(
    name = "daybook",
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
    /// List documents or show details for a specific document
    Docs {
        /// Optional document ID to show details for
        id: Option<String>,
    },
    /// Generate shell completions
    Completions {
        #[clap(value_enum)]
        shell: clap_complete::Shell,
    },
}

