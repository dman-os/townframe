#[allow(unused)]
mod interlude {
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

mod config;
mod context;
// mod fuse;

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
        Commands::Ls | Commands::Cat { .. } | Commands::Touch | Commands::Ed { .. } => {
            let cli_config = config::CliConfig::source().await?;
            let config = context::Config::new(cli_config)?;
            let ctx = context::Ctx::init(config).await?;
            let drawer_doc_id = ctx.doc_drawer().document_id().clone();
            let repo =
                daybook_core::drawer::DrawerRepo::load(ctx.acx.clone(), drawer_doc_id).await?;

            match args.command {
                Commands::Ls => {
                    let doc_ids = repo.list().await;
                    repo.store().query_sync(|store| {
                        debug!("DEBUG: store keys: {:?}", store.map.keys().collect::<Vec<_>>());
                    }).await;
                    let mut docs = Vec::new();
                    for doc_id in &doc_ids {
                        if let Some(doc) = repo.get(doc_id).await? {
                            docs.push((doc_id.clone(), doc));
                        }
                    }

                    use comfy_table::presets::NOTHING;
                    use comfy_table::Table;
                    let mut table = Table::new();
                    table.load_preset(NOTHING).set_header(vec!["ID", "Title"]);
                    for (id, doc) in docs {
                        let title = doc
                            .props
                            .get(&daybook_types::doc::DocPropKey::Tag(
                                daybook_types::doc::WellKnownPropTag::TitleGeneric.into(),
                            ))
                            .and_then(|val| match val {
                                daybook_types::doc::DocProp::WellKnown(
                                    daybook_types::doc::WellKnownProp::TitleGeneric(str),
                                ) => Some(str.clone()),
                                _ => None,
                             })
                            .unwrap_or_else(|| "<no title>".to_string());
                        table.add_row(vec![id, title]);
                    }
                    println!("{table}");
                }
                Commands::Cat { id } => {
                    let doc = repo.get(&id).await?;
                    if let Some(doc) = doc {
                        println!("{}", serde_json::to_string_pretty(&*doc)?);
                    } else {
                        eyre::bail!("Document not found: {id}");
                    }
                }
                Commands::Touch => {
                    let doc = daybook_types::doc::Doc {
                        id: "".into(), // will be assigned by repo
                        created_at: time::OffsetDateTime::now_utc(),
                        updated_at: time::OffsetDateTime::now_utc(),
                        props: {
                            let mut p = HashMap::new();
                            p.insert(
                                daybook_types::doc::WellKnownPropTag::TitleGeneric.into(),
                                daybook_types::doc::WellKnownProp::TitleGeneric("Untitled".into()).into(),
                            );
                            p
                        },
                    };
                    let id = repo.add(doc).await?;
                    println!("Created document: {id}");
                }
                Commands::Ed { id } => {
                    let Some((doc, heads)) = repo.get_with_heads(&id).await? else {
                        eyre::bail!("Document not found: {id}");
                    };

                    let content = serde_json::to_string_pretty(&*doc)?;
                    
                    // Create temporary file
                    // TODO: replace with tempfile crate usage
                    let tmp_dir = std::env::temp_dir();
                    let tmp_path = tmp_dir.join(format!("daybook-edit-{}.json", id));
                    std::fs::write(&tmp_path, &content)?;

                    // Open editor
                    let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_string());
                    let status = std::process::Command::new(editor)
                        .arg(&tmp_path)
                        .status()?;

                    if !status.success() {
                        eyre::bail!("Editor exited with failure");
                    }

                    // Read back and compare
                    let new_content = std::fs::read_to_string(&tmp_path)?;
                    let new_doc: daybook_types::doc::Doc = serde_json::from_str(&new_content)
                        .wrap_err("Failed to parse modified document as JSON")?;

                    let mut patch = daybook_types::doc::Doc::diff(&*doc, &new_doc);
                    if patch.is_empty() {
                        println!("No changes detected.");
                    } else {
                        patch.id = id.clone();
                        repo.update_at_heads(patch, &heads).await?;
                        println!("Updated document: {id}");
                    }

                    // Cleanup
                    let _ = std::fs::remove_file(&tmp_path);
                }
                _ => unreachable!(),
            }
            use daybook_core::repos::Repo;
            repo.stop();
            ctx.acx.stop().await?;
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
    /// List documents
    Ls,
    /// Show details for a specific document
    Cat { id: String },
    /// Create a new document
    Touch,
    /// Edit a document
    Ed { id: String },
    /// Generate shell completions
    Completions {
        #[clap(value_enum)]
        shell: clap_complete::Shell,
    },
}
