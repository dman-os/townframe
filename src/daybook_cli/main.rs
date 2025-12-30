#[allow(unused)]
mod interlude {
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

use std::process::ExitCode;

use clap::Parser;

mod config;
mod context;
// mod fuse;

use clap::builder::styling::AnsiColor;

fn main() -> Res<ExitCode> {
    // dotenv_flow::dotenv_flow().ok();
    utils_rs::setup_tracing()?;

    // the static cli is for commands that
    // can be executed without having to
    // build up the dynamic sections of
    // the CLI into clap reprs
    let static_res = match try_static_cli()? {
        StaticCliResult::Exit(code) => {
            return Ok(code);
        }
        val => val,
    };

    lazy::rt().block_on(dynamic_cli(static_res))
}

fn try_static_cli() -> Res<StaticCliResult> {
    let cli = match Cli::try_parse() {
        Err(err) => {
            let kind = err.kind();
            use clap::error::ErrorKind;
            // these might be possible on the dynamic
            // cli so we don't abort immediately
            if kind == ErrorKind::InvalidSubcommand
                || kind == ErrorKind::InvalidValue
                || kind == ErrorKind::DisplayHelp
                || kind == ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            {
                return Ok(StaticCliResult::ClapErr(err));
            }
            err.exit();
        }
        Ok(args) => args,
    };

    match cli.command {
        StaticCommands::Completions { shell } => {
            // don't handle completions now or the dynamic_cli
            // won't be included
            return Ok(StaticCliResult::Completions(shell));
        }
        _ => {
            // the rest of the commands can be statically handled
        }
    }

    lazy::rt()
        .block_on(static_cli(cli))
        .map(StaticCliResult::Exit)
}

enum StaticCliResult {
    ClapErr(clap::Error),
    Exit(ExitCode),
    Completions(clap_complete::Shell),
}

impl StaticCliResult {
    /// Used for deferred exit after we've built the full cli
    fn exit(self, cmd: Option<&mut clap::Command>) -> ExitCode {
        use clap::CommandFactory;
        use clap_complete::aot::generate;
        match self {
            StaticCliResult::ClapErr(err) => err.exit(),
            StaticCliResult::Completions(shell) => {
                let mut stdout = std::io::stdout();
                generate(
                    shell,
                    cmd.unwrap_or(&mut Cli::command()),
                    "daybook_cli".to_string(),
                    &mut stdout,
                );
                ExitCode::SUCCESS
            }
            StaticCliResult::Exit(_) => unreachable!("can't happen"),
        }
    }
}

async fn static_cli(cli: Cli) -> Res<ExitCode> {
    let cli_config = lazy::cli_config().await?;
    let config = context::Config::new(&cli_config)?;
    let ctx = context::Ctx::init(config).await?;
    let drawer_doc_id = ctx.doc_drawer().document_id().clone();
    let repo = daybook_core::drawer::DrawerRepo::load(ctx.acx.clone(), drawer_doc_id).await?;

    match cli.command {
        StaticCommands::Ls => {
            let doc_ids = repo.list().await;
            repo.store()
                .query_sync(|store| {
                    debug!(
                        "DEBUG: store keys: {:?}",
                        store.map.keys().collect::<Vec<_>>()
                    );
                })
                .await;
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
        StaticCommands::Cat { id } => {
            let doc = repo.get(&id).await?;
            if let Some(doc) = doc {
                println!("{}", serde_json::to_string_pretty(&*doc)?);
            } else {
                eyre::bail!("Document not found: {id}");
            }
        }
        StaticCommands::Touch => {
            let doc = daybook_types::doc::Doc {
                id: "".into(), // will be assigned by repo
                created_at: time::OffsetDateTime::now_utc(),
                updated_at: time::OffsetDateTime::now_utc(),
                props: [
                    //
                    (
                        daybook_types::doc::WellKnownPropTag::TitleGeneric.into(),
                        daybook_types::doc::WellKnownProp::TitleGeneric("Untitled".into()).into(),
                    ),
                ]
                .into(),
            };
            let id = repo.add(doc).await?;
            eprintln!("created document: {id}");
            println!("{id}");
        }
        StaticCommands::Ed { id } => {
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
            let status = std::process::Command::new(editor).arg(&tmp_path).status()?;

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

    Ok(ExitCode::SUCCESS)
}

async fn dynamic_cli(static_res: StaticCliResult) -> Res<ExitCode> {
    let cli_config = lazy::cli_config().await?;
    let config = context::Config::new(&cli_config)?;
    let ctx = context::Ctx::init(config).await?;

    macro_rules! do_cleanup {
        () => {
            ctx.acx.stop().await?;
        };
    }

    // let drawer_doc_id = ctx.doc_drawer().document_id().clone();
    // let drawer_repo =
    //     daybook_core::drawer::DrawerRepo::load(ctx.acx.clone(), drawer_doc_id).await?;

    let plugs_repo =
        daybook_core::plugs::PlugsRepo::load(ctx.acx.clone(), ctx.doc_app().document_id().clone())
            .await
            .wrap_err("error loading plugs repo")?;

    let plugs = plugs_repo.list_plugs().await;

    fn clap_for_command(
        plug_id: &str,
        man: &daybook_core::plugs::manifest::CommandManifest,
    ) -> clap::Command {
        clap::Command::new(man.name.0.clone())
            .long_about(man.desc.clone())
            .before_long_help(format!("From the {plug_id} plug."))
            .styles(CLAP_STYLE)
    }

    #[derive(Debug)]
    struct CommandDeets {
        clap: clap::Command,
        fqcn: String,
        src_plug_id: Arc<str>,
        man: daybook_core::plugs::manifest::CommandManifest,
    }
    // source plug for each command
    let mut command_details: HashMap<String, CommandDeets> = default();
    for plug_man in plugs.iter() {
        let plug_id: Arc<str> = plug_man.id().into();
        info!(?plug_man.commands, "XXX");
        for com_man in plug_man.commands.iter() {
            let details = CommandDeets {
                clap: clap_for_command(&plug_id, com_man),
                fqcn: format!("{plug_id}/{name}", name = &com_man.name[..]),
                man: com_man.clone(),
                src_plug_id: plug_id.clone(),
            };
            // we check for clash of command names first
            if let Some(clash) = command_details.remove(&com_man.name[..]) {
                // we use the fqcn for both clashing items as the command names
                if let Some(old) = command_details.insert(clash.fqcn.clone(), clash) {
                    panic!("fqcn clash: {}", old.fqcn);
                }
                if let Some(old) = command_details.insert(details.fqcn.clone(), details) {
                    panic!("fqcn clash: {}", old.fqcn);
                }
            } else {
                if let Some(old) = command_details.insert(com_man.name.0.clone(), details) {
                    panic!("fqcn clash: {}", old.fqcn);
                }
            }
        }
    }

    use clap::*;
    let mut exec_cmd = clap::Command::new("exec")
        .visible_alias("x")
        .styles(CLAP_STYLE)
        .subcommands(
            command_details
                .iter()
                .map(|(_name, details)| details.clap.clone()),
        );
    let mut root_cmd = Cli::command().subcommand(exec_cmd.clone());

    // if it's already known to be a completions request,
    // no need to prase the argv again
    if let StaticCliResult::Completions(shell) = static_res {
        return Ok(StaticCliResult::Completions(shell).exit(Some(&mut root_cmd)));
    }

    let matches = match root_cmd.try_get_matches() {
        Ok(val) => val,
        Err(err) => {
            err.exit();
        }
    };

    match StaticCommands::from_arg_matches(&matches) {
        Err(err) => {
            let kind = err.kind();
            use clap::error::ErrorKind;
            // these are again, non matching commannds
            // that might be handled by the dynaic cli
            if !(kind == ErrorKind::InvalidSubcommand
                || kind == ErrorKind::InvalidValue
                || kind == ErrorKind::DisplayHelp
                || kind == ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand)
            {
                err.exit();
            }
        }
        Ok(StaticCommands::Completions { .. }) => {
            unreachable!("completions have already been handled");
        }
        Ok(StaticCommands::Ls)
        | Ok(StaticCommands::Touch)
        | Ok(StaticCommands::Cat { .. })
        | Ok(StaticCommands::Ed { .. }) => {
            unreachable!("static_cli will prevent these");
        }
    }

    match matches.subcommand() {
        Some(("exec", sub_matches)) => match sub_matches.subcommand() {
            Some((name, _sub_matches)) => {
                info!(?name, "XXX");
                let details = command_details.remove(name).unwrap();
                info!(?details, "executing");

                do_cleanup!();
                Ok(ExitCode::SUCCESS)
            }
            _ => {
                exec_cmd.print_long_help()?;
                Ok(ExitCode::FAILURE)
            }
        },
        _ => unreachable!("we can't reach this"),
        // _ => root_cmd.print_long_help()?,
    }
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
struct Cli {
    #[clap(subcommand)]
    command: StaticCommands,
}

#[derive(Debug, clap::Subcommand)]
enum StaticCommands {
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

mod lazy {
    use std::sync::OnceLock;

    use crate::interlude::*;

    use crate::config::CliConfig;

    const RT: OnceLock<Res<Arc<tokio::runtime::Runtime>>> = OnceLock::new();

    pub fn rt() -> Arc<tokio::runtime::Runtime> {
        match RT.get_or_init(|| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            eyre::Ok(Arc::new(rt))
        }) {
            Ok(val) => val.clone(),
            Err(err) => panic!("error on tokio init: {err}"),
        }
    }

    pub async fn cli_config() -> Res<Arc<CliConfig>> {
        const CONFIG: tokio::sync::OnceCell<Arc<CliConfig>> = tokio::sync::OnceCell::const_new();
        match CONFIG
            .get_or_try_init(|| async {
                let conf = CliConfig::source().await?;
                eyre::Ok(Arc::new(conf))
            })
            .await
        {
            Ok(config) => {
                debug!("config sourced: {config:?}");
                Ok(config.clone())
            }
            Err(err) => Err(err),
        }
    }
}
