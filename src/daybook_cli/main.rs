#[allow(unused)]
mod interlude {
    pub use utils_rs::prelude::*;

    pub use crate::context::SharedCtx;
}

use crate::interlude::*;

use std::process::ExitCode;

use clap::builder::styling::AnsiColor;
use clap::*;

use daybook_core::blobs::BlobsRepo;
use daybook_core::config::ConfigRepo;
use daybook_core::drawer::DrawerRepo;
use daybook_core::plugs::{manifest, PlugsRepo};
use daybook_core::repos::Repo;
use daybook_core::rt::DispatcherRepo;
use daybook_core::tables::TablesRepo;

mod config;
mod context;
// mod fuse;

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

async fn static_cli(cli: Cli) -> Res<ExitCode> {
    let conf = lazy::config().await?;

    let is_initialized = conf.is_repo_initialized().await?;

    if let StaticCommands::Init {} = cli.command {
        if is_initialized {
            warn!(
                path = ?conf.cli_config.repo_path,
                "initialized repo already found at path"
            );
            return Ok(ExitCode::SUCCESS);
        }
        let ctx = context::Ctx::init(conf.clone()).await?;
        let drawer_doc_id = ctx.doc_drawer().document_id();
        let app_doc_id = ctx.doc_app().document_id();
        let drawer = DrawerRepo::load(ctx.acx.clone(), drawer_doc_id.clone()).await?;
        let plugs_repo = PlugsRepo::load(ctx.acx.clone(), app_doc_id.clone())
            .await
            .wrap_err("error loading plugs repo")?;
        let conf_repo =
            ConfigRepo::load(ctx.acx.clone(), app_doc_id.clone(), plugs_repo.clone()).await?;
        let tables_repo = TablesRepo::load(ctx.acx.clone(), app_doc_id.clone()).await?;
        let dispatcher_repo = DispatcherRepo::load(ctx.acx.clone(), app_doc_id.clone()).await?;
        let _blobs_repo = BlobsRepo::new(conf.cli_config.repo_path.clone()).await?;

        plugs_repo.ensure_system_plugs().await?;

        drawer.stop();
        plugs_repo.stop();
        conf_repo.stop();
        tables_repo.stop();
        dispatcher_repo.stop();

        ctx.acx.stop().await?;
        info!(
            path = ?conf.cli_config.repo_path,
            "repo initialization success"
        );
        return Ok(ExitCode::SUCCESS);
    }

    if !is_initialized {
        error!(
            path = ?conf.cli_config.repo_path,
            "repo not initialized at resolved path",
        );
        return Ok(ExitCode::FAILURE);
    }
    // we only create init the Ctx after checking if the
    // configured repo is Initialized since `init`
    // initializes the repo
    let ctx = context::Ctx::init(conf).await?;
    let drawer_doc_id = ctx.doc_drawer().document_id().clone();
    let drawer = DrawerRepo::load(ctx.acx.clone(), drawer_doc_id).await?;
    scopeguard::defer! {
        drawer.stop()
    };

    match cli.command {
        StaticCommands::Init {} | StaticCommands::Completions { .. } => unreachable!(),
        StaticCommands::Ls => {
            let doc_ids = drawer.list().await;

            drawer
                .store()
                .query_sync(|store| {
                    debug!(
                        "DEBUG: store keys: {:?}",
                        store.map.keys().collect::<Vec<_>>()
                    );
                })
                .await;
            let mut docs = Vec::new();
            for doc_id in &doc_ids {
                if let Some(doc) = drawer.get(doc_id).await? {
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
            let doc = drawer.get(&id).await?;
            if let Some(doc) = doc {
                println!("{:#?}", &doc);
                println!("{}", serde_json::to_string_pretty(&*doc)?);
            } else {
                eyre::bail!("Document not found: {id}");
            }
        }
        StaticCommands::Touch => {
            let doc = daybook_types::doc::Doc {
                id: "".into(), // will be assigned by repo
                created_at: Timestamp::now(),
                updated_at: Timestamp::now(),
                props: [
                    //
                    (
                        daybook_types::doc::WellKnownPropTag::TitleGeneric.into(),
                        daybook_types::doc::WellKnownProp::TitleGeneric("Untitled".into()).into(),
                    ),
                ]
                .into(),
            };
            let id = drawer.add(doc).await?;
            eprintln!("created document: {id}");
            println!("{id}");
        }
        StaticCommands::Ed { id } => {
            let Some((doc, heads)) = drawer.get_with_heads(&id).await? else {
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
                drawer.update_at_heads(patch, &heads).await?;
                println!("Updated document: {id}");
            }

            // Cleanup
            let _ = std::fs::remove_file(&tmp_path);
        }
    }
    ctx.acx.stop().await?;

    Ok(ExitCode::SUCCESS)
}

async fn dynamic_cli(static_res: StaticCliResult) -> Res<ExitCode> {
    let conf = lazy::config().await?;

    let mut root_cmd = Cli::command();

    // if we don't have an Initialized repo, we can't really
    // do a dynamic cli so we terminate early
    if !conf.is_repo_initialized().await? {
        error!(
            path = ?conf.cli_config.repo_path,
            "repo not initialized at resolved path",
        );
        let code = static_res.exit(Some(&mut root_cmd));
        error!(
            path = ?conf.cli_config.repo_path,
            "repo not initialized at resolved path",
        );
        return Ok(code);
    }

    let ctx = context::Ctx::init(conf).await?;
    macro_rules! do_cleanup {
        () => {
            ctx.acx.stop().await?;
        };
    }

    // let drawer_doc_id = ctx.doc_drawer().document_id().clone();
    // let drawer_repo =
    //     daybook_core::drawer::DrawerRepo::load(ctx.acx.clone(), drawer_doc_id).await?;

    let plugs_repo = PlugsRepo::load(ctx.acx.clone(), ctx.doc_app().document_id().clone())
        .await
        .wrap_err("error loading plugs repo")?;
    scopeguard::defer! {
        plugs_repo.stop();
    };

    let plugs = plugs_repo.list_plugs().await;

    // source plug for each command
    let mut command_details: HashMap<String, ClapReadyCommand> = default();
    for plug_man in plugs.iter() {
        let plug_id: Arc<str> = plug_man.id().into();
        for com_man in plug_man.commands.iter() {
            let details = ready_command_clap(plug_id.clone(), plug_man, com_man)?;

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

    let mut exec_cmd = clap::Command::new("exec")
        .visible_alias("x")
        .styles(CLAP_STYLE)
        .subcommands(
            command_details
                .iter()
                .map(|(_name, details)| details.clap.clone()),
        );

    root_cmd = root_cmd.subcommand(exec_cmd.clone());

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
        | Ok(StaticCommands::Init { .. })
        | Ok(StaticCommands::Cat { .. })
        | Ok(StaticCommands::Ed { .. }) => {
            unreachable!("static_cli will prevent these");
        }
    }
    match matches.subcommand() {
        Some(("exec", sub_matches)) => match sub_matches.subcommand() {
            Some((name, sub_matches)) => {
                info!(?name, "XXX");
                let details = command_details.remove(name).unwrap();
                info!(?details, "executing");

                let drawer =
                    DrawerRepo::load(ctx.acx.clone(), ctx.doc_drawer().document_id().clone())
                        .await?;
                let ecx = ExecCtx {
                    cx: ctx.clone(),
                    drawer,
                };

                (details.action)(sub_matches.clone(), ecx).await?;

                do_cleanup!();
                Ok(ExitCode::SUCCESS)
            }
            _ => {
                do_cleanup!();
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
    // Initialize repo
    Init {},
    /// List documents
    Ls,
    /// Show details for a specific document
    Cat {
        id: String,
    },
    /// Create a new document
    Touch,
    /// Edit a document
    Ed {
        id: String,
    },
    /// Generate shell completions
    Completions {
        #[clap(value_enum)]
        shell: clap_complete::Shell,
    },
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

struct ExecCtx {
    cx: SharedCtx,
    drawer: Arc<DrawerRepo>,
}

#[derive(educe::Educe)]
#[educe(Debug)]
struct ClapReadyCommand {
    pub clap: clap::Command,
    pub fqcn: String,
    pub src_plug_id: Arc<str>,
    pub man: Arc<manifest::CommandManifest>,
    #[educe(Debug(ignore))]
    pub action: CliCommandAction,
}

type CliCommandAction = Box<
    dyn Fn(clap::ArgMatches, ExecCtx) -> futures::future::BoxFuture<'static, Res<()>> + Send + Sync,
>;

fn ready_command_clap(
    plug_id: Arc<str>,
    plug_man: &Arc<manifest::PlugManifest>,
    com_man: &Arc<manifest::CommandManifest>,
) -> Res<ClapReadyCommand> {
    let mut clap_cmd = clap::Command::new(com_man.name.0.clone())
        .long_about(com_man.desc.clone())
        .before_help(format!("From the {plug_id} plug."))
        .styles(CLAP_STYLE);

    let action = match &com_man.deets {
        manifest::CommandDeets::DocCommand { routine_name } => {
            let routine = plug_man.routines.get(routine_name).ok_or_else(|| {
                ferr!(
                    "routine not found '{routine_name}' specified by command \
                            '{cmd_name}' not found",
                    cmd_name = com_man.name.0
                )
            })?;
            clap_cmd = clap_cmd
                .after_help(format!(
                    "Command type: DocCommand
Routine name: {routine_name}
Routine deets: {routine_deets:?}
Routine acl: {routine_acl:?}
Routine impl: {routine_impl:?}
",
                    routine_deets = routine.deets,
                    routine_acl = routine.prop_acl,
                    routine_impl = routine.r#impl,
                ))
                .arg(Arg::new("doc-id").required(true));

            Box::new({
                let com_man = com_man.clone();
                move |matches: ArgMatches, ecx: ExecCtx| {
                    async move {
                        let doc_id = matches
                            .get_one::<String>("doc-id")
                            .expect("this shouldn't happen");

                        let doc = ecx.drawer.get(doc_id).await?;

                        println!("{doc:?}");

                        Ok(())
                    }
                    .boxed()
                }
            })
        }
    };

    Ok(ClapReadyCommand {
        clap: clap_cmd,
        fqcn: format!("{plug_id}/{name}", name = &com_man.name[..]),
        man: com_man.clone(),
        src_plug_id: plug_id.clone(),
        action,
    })
}

mod lazy {
    use std::sync::OnceLock;

    use crate::interlude::*;

    use crate::config::CliConfig;
    use crate::context::*;

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

    pub async fn config() -> Res<Arc<Config>> {
        const CONFIG: tokio::sync::OnceCell<Arc<Config>> = tokio::sync::OnceCell::const_new();
        match CONFIG
            .get_or_try_init(|| async {
                let cli_config = cli_config().await?;
                let conf = Config::new(cli_config)?;
                eyre::Ok(Arc::new(conf))
            })
            .await
        {
            Ok(config) => Ok(config.clone()),
            Err(err) => Err(err),
        }
    }
}
