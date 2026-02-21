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
use daybook_core::progress::ProgressRepo;
use daybook_core::rt::dispatch::DispatchRepo;

mod config;
mod context;

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
        let ctx = context::open_repo_ctx(&conf, true, None).await?;
        if let Some(stop) = ctx.acx_stop.lock().await.take() {
            stop.stop().await?;
        }
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
    let ctx = context::open_repo_ctx(&conf, false, None).await?;
    let drawer_doc_id = ctx.doc_drawer.document_id().clone();
    let (drawer, drawer_stop) = DrawerRepo::load(
        ctx.acx.clone(),
        drawer_doc_id,
        ctx.local_actor_id.clone(),
        Arc::new(std::sync::Mutex::new(
            daybook_core::drawer::lru::KeyedLruPool::new(1000),
        )),
        Arc::new(std::sync::Mutex::new(
            daybook_core::drawer::lru::KeyedLruPool::new(1000),
        )),
    )
    .await?;

    match cli.command {
        StaticCommands::Init {} | StaticCommands::Completions { .. } => unreachable!(),
        StaticCommands::Dump => {
            let mut drawer = ctx.doc_drawer.with_document(|doc| {
                // eyre::Ok(doc.hydrate(None))
                let value: ThroughJson<serde_json::Value> = autosurgeon::hydrate(doc)?;
                eyre::Ok(value.0)
            })?;
            let mut app = ctx.doc_app.with_document(|doc| {
                // eyre::Ok(doc.hydrate(None))
                let value: ThroughJson<serde_json::Value> = autosurgeon::hydrate(doc)?;
                eyre::Ok(value.0)
            })?;
            fn display_byte_array(val: &mut serde_json::Value) {
                match val {
                    serde_json::Value::Array(values) => {
                        if values
                            .iter()
                            .all(|val| matches!(val, serde_json::Value::Number(..)))
                        {
                            *val = serde_json::Value::String(
                                format!("byte array, len = {}", values.len()).into(),
                            )
                        } else {
                            for val in values {
                                display_byte_array(val);
                            }
                        }
                    }
                    serde_json::Value::Object(map) => {
                        for (_, val) in map {
                            display_byte_array(val)
                        }
                    }
                    _ => {}
                }
            }
            display_byte_array(&mut drawer);
            display_byte_array(&mut app);
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "drawer": drawer,
                    "app": app,
                }))?
            )
        }
        StaticCommands::Ls => {
            let doc_entries = drawer.list().await?;

            let mut docs = Vec::new();
            for entry in &doc_entries {
                let Some(main_branch) = entry.main_branch_path() else {
                    warn!(doc_id = ?entry.doc_id,"no branches found on doc");
                    continue;
                };
                if let Some(doc) = drawer
                    .get_doc_with_facets_at_branch(&entry.doc_id, &main_branch, None)
                    .await?
                {
                    docs.push((entry.clone(), doc));
                }
            }

            use comfy_table::presets::NOTHING;
            use comfy_table::Table;
            use daybook_types::doc::{WellKnownFacet, WellKnownFacetTag};

            let mut table = Table::new();
            table
                .load_preset(NOTHING)
                .set_header(vec!["ID", "Title", "Branches"]);

            for (entry, doc) in docs {
                let title = doc
                    .facets
                    .get(&WellKnownFacetTag::TitleGeneric.into())
                    .map(|val| {
                        match WellKnownFacet::from_json(
                            val.clone(),
                            WellKnownFacetTag::TitleGeneric,
                        ) {
                            Ok(WellKnownFacet::TitleGeneric(str)) => str.clone(),
                            _ => panic!("tag - facet mismatch"),
                        }
                    })
                    .unwrap_or_else(|| "<no title>".to_string());
                table.add_row(vec![
                    entry.doc_id,
                    title,
                    entry
                        .branches
                        .keys()
                        .map(|key| key.as_str())
                        .collect::<Vec<_>>()
                        .join(","),
                ]);
            }
            println!("{table}");
        }
        StaticCommands::Cat { id, branch } => {
            let Ok(Some(branches)) = drawer.get_doc_branches(&id).await else {
                error!("document not found: {id}");
                return Ok(ExitCode::FAILURE);
            };
            let branch_path = match &branch {
                Some(val) => {
                    if !branches.branches.contains_key(val) {
                        error!("branch not found for doc: {id} - {val}");
                        return Ok(ExitCode::FAILURE);
                    }
                    daybook_types::doc::BranchPath::from(val.as_str())
                }
                None => {
                    let Some(branch) = branches.main_branch_path() else {
                        error!(doc_id = ?branches.doc_id,"no branches found on doc");
                        return Ok(ExitCode::FAILURE);
                    };
                    branch
                }
            };
            let doc = drawer
                .get_doc_with_facets_at_branch(&id, &branch_path, None)
                .await?
                .expect("document from entry missing");
            println!("{:#?}", &doc);
            println!("{}", serde_json::to_string_pretty(&*doc)?);
        }
        StaticCommands::Touch => {
            let doc = daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPath::from("main"),
                facets: [
                    //
                    (
                        daybook_types::doc::WellKnownFacetTag::TitleGeneric.into(),
                        daybook_types::doc::WellKnownFacet::TitleGeneric("Untitled".into()).into(),
                    ),
                ]
                .into(),
                user_path: Some(daybook_types::doc::UserPath::from(
                    ctx.local_user_path.clone(),
                )),
            };
            let id = drawer.add(doc).await?;
            info!(id, "created document");
            println!("{id}");
        }
        StaticCommands::Ed { id, branch } => {
            let Ok(Some(branches)) = drawer.get_doc_branches(&id).await else {
                error!("document not found: {id}");
                return Ok(ExitCode::FAILURE);
            };
            let branch_path = match &branch {
                Some(val) => {
                    if branches.branches.contains_key(val) {
                        error!("branch not found for doc: {id} - {val}");
                        return Ok(ExitCode::FAILURE);
                    }
                    daybook_types::doc::BranchPath::from(val.as_str())
                }
                None => {
                    let Some(branch) = branches.main_branch_path() else {
                        error!(doc_id = ?branches.doc_id,"no branches found on doc");
                        return Ok(ExitCode::FAILURE);
                    };
                    branch
                }
            };
            let Some((doc, heads)) = drawer.get_with_heads(&id, &branch_path, None).await? else {
                eyre::bail!("Document not found: {id}");
            };

            let content = serde_json::to_string_pretty(&*doc)?;

            // Create temporary file
            // TODO: replace with tempfile crate usage
            let tmp_dir = std::env::temp_dir();
            let tmp_path = tmp_dir.join(format!("daybook-edit-{}.json", id));
            tokio::fs::write(&tmp_path, &content).await?;

            // Open editor
            let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_string());
            let status = std::process::Command::new(editor).arg(&tmp_path).status()?;

            if !status.success() {
                eyre::bail!("Editor exited with failure");
            }

            // Read back and compare
            let new_content = tokio::fs::read_to_string(&tmp_path).await?;
            let new_doc: daybook_types::doc::Doc = serde_json::from_str(&new_content)
                .wrap_err("Failed to parse modified document as JSON")?;

            let mut patch = daybook_types::doc::Doc::diff(&doc, &new_doc);
            if patch.is_empty() {
                println!("No changes detected.");
            } else {
                patch.id = id.clone();
                drawer
                    .update_at_heads(patch, "main".into(), Some(heads))
                    .await?;
                println!("Updated document: {id}");
            }

            // Cleanup
            tokio::fs::remove_file(&tmp_path).await?;
        }
        StaticCommands::Livetree { command } => {
            let root_path = conf.cli_config.repo_path.join("livetree");
            let metadata_db_path = conf
                .cli_config
                .repo_path
                .join("pauperfuse")
                .join("livetree.sqlite");
            let mut livetree_cx = daybook_fuse::DaybookFuseCtx::new(
                daybook_fuse::Config {
                    root_path,
                    metadata_db_path,
                    branch_path: daybook_types::doc::BranchPath::from("main"),
                    poll_interval: std::time::Duration::from_millis(250),
                },
                Arc::clone(&drawer),
            );

            match command {
                LivetreeCommands::Init {} => {
                    daybook_fuse::bootstrap_livetree(&mut livetree_cx).await?;
                    println!(
                        "livetree initialized at {}",
                        livetree_cx.config.root_path.display()
                    );
                }
                LivetreeCommands::Status {} => {
                    let status = daybook_fuse::status(&mut livetree_cx).await?;
                    println!(
                        "in-sync: {}, provider-only: {}, backend-only: {}, diverged: {}, scanned: {}, changed: {}",
                        status.in_sync_count,
                        status.provider_only_count,
                        status.backend_only_count,
                        status.diverged_count,
                        status.scanned_doc_count,
                        status.changed_doc_count
                    );
                }
                LivetreeCommands::Pull {} => {
                    let report = daybook_fuse::pull_changes(&mut livetree_cx).await?;
                    println!(
                        "pull complete: provider_deltas={}, effects={}, scanned={}, changed={}",
                        report.provider_delta_count,
                        report.effect_count,
                        report.scanned_doc_count,
                        report.changed_doc_count
                    );
                }
                LivetreeCommands::Push {} => {
                    let report = daybook_fuse::push_changes(&mut livetree_cx).await?;
                    println!(
                        "push complete: backend_deltas={}, effects={}, scanned={}, changed={}",
                        report.backend_delta_count,
                        report.effect_count,
                        report.scanned_doc_count,
                        report.changed_doc_count
                    );
                }
                LivetreeCommands::Reconcile {} => {
                    let report = daybook_fuse::reconcile_once(&mut livetree_cx).await?;
                    println!(
                        "reconcile complete: backend_deltas={}, provider_deltas={}, effects={}, scanned={}, changed={}",
                        report.backend_delta_count,
                        report.provider_delta_count,
                        report.effect_count,
                        report.scanned_doc_count,
                        report.changed_doc_count
                    );
                }
            }
        }
    }
    drawer_stop.stop().await?;
    if let Some(stop) = ctx.acx_stop.lock().await.take() {
        stop.stop().await?;
    }

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

    let ctx = context::open_repo_ctx(&conf, false, None).await?;
    let blobs_repo = BlobsRepo::new(ctx.layout.blobs_root.clone()).await?;
    let (plugs_repo, plugs_stop) = PlugsRepo::load(
        ctx.acx.clone(),
        Arc::clone(&blobs_repo),
        ctx.doc_app.document_id().clone(),
        ctx.local_actor_id.clone(),
    )
    .await?;
    let (
        //
        (drawer, drawer_stop),
        (dispatch_repo, dispatch_stop),
        progress_repo,
        (config_repo, config_stop),
    ) = tokio::try_join!(
        DrawerRepo::load(
            ctx.acx.clone(),
            ctx.doc_drawer.document_id().clone(),
            ctx.local_actor_id.clone(),
            Arc::new(std::sync::Mutex::new(
                daybook_core::drawer::lru::KeyedLruPool::new(1000)
            )),
            Arc::new(std::sync::Mutex::new(
                daybook_core::drawer::lru::KeyedLruPool::new(1000)
            )),
        ),
        DispatchRepo::load(
            ctx.acx.clone(),
            ctx.doc_app.document_id().clone(),
            ctx.local_actor_id.clone()
        ),
        ProgressRepo::boot(ctx.sql.db_pool.clone()),
        ConfigRepo::load(
            ctx.acx.clone(),
            ctx.doc_app.document_id().clone(),
            Arc::clone(&plugs_repo),
            daybook_types::doc::UserPath::from(ctx.local_user_path.clone()),
        )
    )?;
    drawer.set_plugs_repo(Arc::clone(&plugs_repo));

    macro_rules! do_cleanup {
        () => {
            drawer_stop.stop().await?;
            plugs_stop.stop().await?;
            dispatch_stop.stop().await?;
            config_stop.stop().await?;
            if let Some(stop) = ctx.acx_stop.lock().await.take() {
                stop.stop().await?;
            }
        };
    }

    let plugs = plugs_repo.list_plugs().await;

    // source plug for each command
    let mut command_details: HashMap<String, PlugCmdClap> = default();
    for plug_man in plugs.iter() {
        let plug_id: Arc<str> = plug_man.id().into();
        for (com_name, com_man) in plug_man.commands.iter() {
            let details = plug_cmd_to_clap(Arc::clone(&plug_id), plug_man, &com_name.0, com_man)?;

            // we check for clash of command names first
            if let Some(clash) = command_details.remove(&com_name.0[..]) {
                // we use the fqcn for both clashing items as the command names
                if let Some(old) = command_details.insert(clash.fqcn.clone(), clash) {
                    panic!("fqcn clash: {}", old.fqcn);
                }
                if let Some(old) = command_details.insert(details.fqcn.clone(), details) {
                    panic!("fqcn clash: {}", old.fqcn);
                }
            } else {
                if let Some(old) = command_details.insert(com_name.0.clone(), details) {
                    panic!("fqcn clash: {}", old.fqcn);
                }
            }
        }
    }

    let mut exec_cmd = clap::Command::new("exec")
        .visible_alias("x")
        .styles(CLAP_STYLE)
        .subcommands(command_details.values().map(|details| details.clap.clone()));

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
        Ok(StaticCommands::Dump)
        | Ok(StaticCommands::Ls)
        | Ok(StaticCommands::Touch)
        | Ok(StaticCommands::Init { .. })
        | Ok(StaticCommands::Cat { .. })
        | Ok(StaticCommands::Ed { .. })
        | Ok(StaticCommands::Livetree { .. }) => {
            unreachable!("static_cli will prevent these");
        }
    }
    match matches.subcommand() {
        Some(("exec", sub_matches)) => match sub_matches.subcommand() {
            Some((name, sub_matches)) => {
                info!(?name, "XXX");
                let details = command_details.remove(name).unwrap();
                let (rt, rt_stop) = daybook_core::rt::Rt::boot(
                    daybook_core::rt::RtConfig {
                        device_id: "main_TODO_XXX".into(),
                    },
                    ctx.doc_app.document_id().clone(),
                    format!("sqlite://{}", ctx.layout.sqlite_path.display()),
                    ctx.acx.clone(),
                    Arc::clone(&drawer),
                    Arc::clone(&plugs_repo),
                    Arc::clone(&dispatch_repo),
                    Arc::clone(&progress_repo),
                    Arc::clone(&blobs_repo),
                    Arc::clone(&config_repo),
                    ctx.local_actor_id.clone(),
                    conf.cli_config.repo_path.join("local_state"),
                )
                .await?;
                let ecx = ExecCtx {
                    rt: Arc::clone(&rt),
                    _cx: Arc::clone(&ctx),
                    drawer: Arc::clone(&drawer),
                };

                let res = (details.action)(sub_matches.clone(), ecx).await;

                rt_stop.stop().await?;
                do_cleanup!();

                res?;

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
    // dump full automerge contents
    Dump,
    /// List documents
    Ls,
    /// Show details for a specific document
    Cat {
        id: String,
        #[arg(short, long)]
        branch: Option<String>,
    },
    /// Create a new document
    Touch,
    /// Edit a document
    Ed {
        id: String,
        #[arg(short, long)]
        branch: Option<String>,
    },
    /// Work with the pauperfuse livetree materialization
    Livetree {
        #[clap(subcommand)]
        command: LivetreeCommands,
    },
    /// Generate shell completions
    Completions {
        #[clap(value_enum)]
        shell: clap_complete::Shell,
    },
}

#[derive(Debug, clap::Subcommand)]
enum LivetreeCommands {
    /// Initialize/materialize the livetree from drawer state
    Init {},
    /// Show sync state between drawer and livetree files
    Status {},
    /// Apply provider (drawer) changes into livetree files
    Pull {},
    /// Apply livetree file changes into provider (drawer)
    Push {},
    /// Push then pull in one cycle
    Reconcile {},
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
    _cx: SharedCtx,
    rt: Arc<daybook_core::rt::Rt>,
    drawer: Arc<DrawerRepo>,
}

#[derive(educe::Educe)]
#[educe(Debug)]
struct PlugCmdClap {
    pub clap: clap::Command,
    pub fqcn: String,
    pub src_plug_id: Arc<str>,
    pub man: Arc<manifest::CommandManifest>,
    #[educe(Debug(ignore))]
    pub action: CliCommandAction,
}

type CliCommandAction = Box<
    dyn FnOnce(clap::ArgMatches, ExecCtx) -> futures::future::BoxFuture<'static, Res<()>>
        + Send
        + Sync,
>;

fn plug_cmd_to_clap(
    plug_id: Arc<str>,
    plug_man: &Arc<manifest::PlugManifest>,
    com_name: &str,
    com_man: &Arc<manifest::CommandManifest>,
) -> Res<PlugCmdClap> {
    let mut clap_cmd = clap::Command::new(com_name.to_string())
        .long_about(com_man.desc.clone())
        .before_help(format!("From the {plug_id} plug."))
        .styles(CLAP_STYLE);

    let action = match &com_man.deets {
        manifest::CommandDeets::DocCommand { routine_name } => {
            let routine = plug_man.routines.get(routine_name).ok_or_else(|| {
                ferr!(
                    "routine not found '{routine_name}' specified by command \
                            '{cmd_name}' not found",
                    cmd_name = com_name
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
                    routine_acl = routine.facet_acl,
                    routine_impl = routine.r#impl,
                ))
                .arg(Arg::new("doc-id").required(true))
                .arg(Arg::new("branch").short('b'));

            Box::new({
                // let com_man = com_man.clone();
                let plug_id = Arc::clone(&plug_id);
                let routine_name = routine_name.0.clone();
                move |matches: ArgMatches, ecx: ExecCtx| {
                    async move {
                        let doc_id = matches
                            .get_one::<String>("doc-id")
                            .expect("this shouldn't happen");
                        let branch = matches.get_one::<String>("branch");
                        let Ok(Some(branches)) = ecx.drawer.get_doc_branches(doc_id).await else {
                            eyre::bail!("document not found: {doc_id}");
                        };
                        let branch_path = match branch {
                            Some(val) => {
                                if branches.branches.contains_key(val) {
                                    eyre::bail!("branch not found for doc: {doc_id} - {val}");
                                }
                                daybook_types::doc::BranchPath::from(val.as_str())
                            }
                            None => {
                                let Some(branch) = branches.main_branch_path() else {
                                    eyre::bail!("no branches found on doc: {doc_id}");
                                };
                                branch
                            }
                        };
                        let heads = branches
                            .branches
                            .get(&branch_path.to_string_lossy().to_string())
                            .unwrap();

                        let job_id = ecx
                            .rt
                            .dispatch(
                                &plug_id,
                                &routine_name[..],
                                daybook_core::rt::DispatchArgs::DocFacet {
                                    doc_id: doc_id.clone(),
                                    branch_path: branch_path.clone(),
                                    heads: heads.clone(),
                                    facet_key: None,
                                },
                            )
                            .await?;
                        ecx.rt
                            .wait_for_dispatch_end(&job_id, std::time::Duration::from_secs(60))
                            .await?;

                        Ok(())
                    }
                    .boxed()
                }
            })
        }
    };

    Ok(PlugCmdClap {
        clap: clap_cmd,
        fqcn: format!("{plug_id}/{name}", name = com_name),
        man: Arc::clone(com_man),
        src_plug_id: Arc::clone(&plug_id),
        action,
    })
}

mod lazy {
    use std::sync::OnceLock;

    use crate::interlude::*;

    use crate::config::CliConfig;
    use crate::context::*;

    static RT: OnceLock<Res<Arc<tokio::runtime::Runtime>>> = OnceLock::new();

    pub fn rt() -> Arc<tokio::runtime::Runtime> {
        match RT.get_or_init(|| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            eyre::Ok(Arc::new(rt))
        }) {
            Ok(val) => Arc::clone(val),
            Err(err) => panic!("error on tokio init: {err}"),
        }
    }

    pub async fn cli_config() -> Res<Arc<CliConfig>> {
        static CONFIG: tokio::sync::OnceCell<Arc<CliConfig>> = tokio::sync::OnceCell::const_new();
        match CONFIG
            .get_or_try_init(|| async {
                let conf = CliConfig::source().await?;
                eyre::Ok(Arc::new(conf))
            })
            .await
        {
            Ok(config) => {
                debug!(?config, "config sourced");
                Ok(Arc::clone(config))
            }
            Err(err) => Err(err),
        }
    }

    pub async fn config() -> Res<Arc<Config>> {
        static CONFIG: tokio::sync::OnceCell<Arc<Config>> = tokio::sync::OnceCell::const_new();
        match CONFIG
            .get_or_try_init(|| async {
                let cli_config = cli_config().await?;
                let conf = Config::new(cli_config).await?;
                eyre::Ok(Arc::new(conf))
            })
            .await
        {
            Ok(config) => Ok(Arc::clone(config)),
            Err(err) => Err(err),
        }
    }
}
