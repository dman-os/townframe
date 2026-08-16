#![recursion_limit = "256"]

mod interlude {
    pub use am_utils_rs::prelude::*;
    pub use utils_rs::prelude::*;

    pub use std::process::ExitCode;

    pub use crate::context::SharedCtx;
    pub(crate) use crate::lazy;
}

use crate::interlude::*;

use clap::builder::styling::AnsiColor;
use clap::*;

mod cmds;
mod config;
mod context;
mod lazy;

fn main() -> Res<ExitCode> {
    // dotenv_flow::dotenv_flow().ok();
    utils_rs::setup_tracing()?;

    // the static cli is for commands that
    // can be executed without having to
    // build up the dynamic sections of
    // the CLI into clap reprs
    let static_res = match try_static_cli() {
        Ok(StaticCliResult::Exit(code)) => {
            lazy::rt().block_on(lazy::shutdown())?;
            return Ok(code);
        }
        Ok(val) => val,
        Err(err) => {
            lazy::rt().block_on(lazy::shutdown())?;
            return Err(err);
        }
    };

    let res = lazy::rt().block_on(async {
        tokio::select! {
            // FIXME: is this a good usage of ctrl_c?
            _ = tokio::signal::ctrl_c() => {
                Ok(ExitCode::FAILURE)
            }
            res = dynamic_cli(static_res) => res,
        }
    });
    lazy::rt().block_on(lazy::shutdown())?;
    res
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
        .block_on(async {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    Ok(ExitCode::FAILURE)
                }
                res = static_cli(cli) => res,
            }
        })
        .map(StaticCliResult::Exit)
}

async fn static_cli(cli: Cli) -> Res<ExitCode> {
    match cli.command {
        StaticCommands::Init {} => {
            return cmds::init::run().await;
        }
        StaticCommands::Clone {
            source,
            destination,
        } => {
            return cmds::clone::run(source, destination).await;
        }
        StaticCommands::Server => {
            return cmds::server::run().await;
        }
        _ => {}
    }

    let conf = lazy::config().await?;
    let is_initialized = conf.is_repo_initialized().await?;

    if !is_initialized {
        error!(
            path = ?conf.cli_config.repo_path,
            "repo not initialized at resolved path",
        );
        return Ok(ExitCode::FAILURE);
    }

    match cli.command {
        StaticCommands::Init {}
        | StaticCommands::Clone { .. }
        | StaticCommands::Completions { .. }
        | StaticCommands::Server => unreachable!(),
        StaticCommands::Dump => cmds::dump::run().await,
        StaticCommands::Ls => cmds::ls::run().await,
        StaticCommands::Cat { id, branch } => cmds::cat::run(id, branch).await,
        StaticCommands::Touch => cmds::touch::run().await,
        StaticCommands::Ed { id, branch } => cmds::ed::run(id, branch).await,
        StaticCommands::Sync {
            sync_urls,
            exit_when_synced,
        } => cmds::sync::run(sync_urls, exit_when_synced).await,
        StaticCommands::Devices { command } => cmds::devices::run(command).await,
    }
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

    // let ctx = Box::pin(lazy::repo_ctx()).await?;
    // let drawer = Box::pin(lazy::drawer_repo()).await?;
    // let plugs_repo = Box::pin(lazy::plugs_repo()).await?;

    let exec_cmd = cmds::exec::Ctx::new().await?;

    root_cmd = root_cmd.subcommand(exec_cmd.subcmd());

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
        | Ok(StaticCommands::Clone { .. })
        | Ok(StaticCommands::Cat { .. })
        | Ok(StaticCommands::Ed { .. })
        | Ok(StaticCommands::Devices { .. })
        | Ok(StaticCommands::Sync { .. })
        | Ok(StaticCommands::Server) => {
            unreachable!("static_cli will prevent these");
        }
    }
    match matches.subcommand() {
        Some(("exec", sub_matches)) => cmds::exec::run(exec_cmd, sub_matches).await,
        _ => unreachable!(""),
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
    /// Clone a repo to a destination path
    Clone {
        /// Source clone URL: db+iroh-clone:<endpoint-ticket>
        source: String,
        /// Destination directory path (must be empty or non-existent)
        destination: String,
    },
    /// Dump full automerge contents
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
    /// Run one-shot iroh sync session
    Sync {
        /// Additional sync URLs to connect to (not persisted)
        sync_urls: Vec<String>,
        /// Exit once the requested peers are synced
        #[arg(long, default_value_t = false)]
        exit_when_synced: bool,
    },
    /// Manage known sync devices
    Devices {
        #[clap(subcommand)]
        command: cmds::devices::DevicesCommands,
    },
    /// Run the btress_auth service host (playground)
    Server,
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

#[cfg(test)]
mod tests {
    use super::*;
    use daybook_core::blobs::BlobsRepo;
    use daybook_core::config::ConfigRepo;
    use daybook_core::drawer::DrawerRepo;
    use daybook_core::index::DocBlobsIndexRepo;
    use daybook_core::local_state::SqliteLocalStateRepo;
    use daybook_core::plugs::PlugsRepo;
    use daybook_core::progress::ProgressRepo;
    use daybook_core::repo::{RepoCtx, RepoOpenOptions};
    use daybook_core::repos::RepoStopToken;
    use daybook_core::sync::IrohSyncRepo;
    use std::collections::HashSet;
    use std::sync::Arc;

    struct CliSyncNode {
        ctx: Arc<RepoCtx>,
        drawer: Arc<DrawerRepo>,
        sync_repo: Arc<IrohSyncRepo>,
        sync_stop: daybook_core::sync::IrohSyncRepoStopToken,
        progress_stop: daybook_core::repos::RepoStopToken,
        plugs_stop: daybook_core::repos::RepoStopToken,
        drawer_stop: daybook_core::repos::RepoStopToken,
        config_stop: daybook_core::repos::RepoStopToken,
        doc_blobs_index_stop: daybook_core::repos::RepoStopToken,
        sqlite_local_state_stop: RepoStopToken,
    }

    impl CliSyncNode {
        async fn stop(self) -> Res<()> {
            let CliSyncNode {
                ctx,
                drawer: _drawer,
                sync_repo,
                sync_stop,
                progress_stop,
                plugs_stop,
                drawer_stop,
                config_stop,
                doc_blobs_index_stop,
                sqlite_local_state_stop,
            } = self;
            sync_stop.stop().await?;
            drop(sync_repo);
            progress_stop.stop().await?;
            doc_blobs_index_stop.stop().await?;
            sqlite_local_state_stop.stop().await?;
            config_stop.stop().await?;
            drawer_stop.stop().await?;
            plugs_stop.stop().await?;
            ctx.shutdown().await?;
            Ok(())
        }
    }

    async fn list_doc_ids(drawer: &DrawerRepo) -> Res<HashSet<String>> {
        let (_, ids) = drawer.list_just_ids().await?;
        Ok(ids.into_iter().collect())
    }

    async fn open_cli_sync_node(repo_root: &std::path::Path) -> Res<CliSyncNode> {
        let ctx = RepoCtx::open(
            repo_root,
            RepoOpenOptions::default(),
            "cli-test-device".into(),
        )
        .await?;
        let blobs_repo = BlobsRepo::new(
            ctx.layout.blobs_root.clone(),
            ctx.local_user_path.clone(),
            Arc::new(daybook_core::blobs::PartitionStoreMembershipWriter::new(
                Arc::clone(&ctx.part_store),
            )),
        )
        .await?;
        let (plugs_repo, plugs_stop) = PlugsRepo::load(
            Arc::clone(&ctx.big_repo),
            Arc::clone(&blobs_repo),
            ctx.doc_app.document_id(),
            daybook_types::doc::UserPathBuf::from(ctx.local_user_path.clone()),
        )
        .await?;
        let (drawer_repo, drawer_stop) = DrawerRepo::load(
            Arc::clone(&ctx.big_repo),
            Arc::clone(&ctx.part_store),
            ctx.doc_drawer.document_id(),
            ctx.local_user_path.clone(),
            ctx.sql.clone(),
            ctx.layout.repo_root.join("local_state"),
            Arc::new(surelock::mutex::Mutex::new(
                utils_rs::lru::KeyedLruPool::new(1000),
            )),
            Arc::new(surelock::mutex::Mutex::new(
                utils_rs::lru::KeyedLruPool::new(1000),
            )),
            Arc::clone(&plugs_repo),
        )
        .await?;
        let (config_repo, config_stop) = ConfigRepo::load(
            Arc::clone(&ctx.big_repo),
            ctx.doc_app.document_id(),
            Arc::clone(&plugs_repo),
            daybook_types::doc::UserPathBuf::from(ctx.local_user_path.clone()),
            ctx.sql.clone(),
        )
        .await?;
        let (sqlite_local_state_repo, sqlite_local_state_stop) =
            SqliteLocalStateRepo::boot(ctx.layout.repo_root.join("local_state")).await?;
        let (doc_blobs_index_repo, doc_blobs_index_stop) = DocBlobsIndexRepo::boot(
            Arc::clone(&drawer_repo),
            Arc::clone(&blobs_repo),
            Arc::clone(&sqlite_local_state_repo),
        )
        .await?;
        let (progress_repo, progress_stop) = ProgressRepo::boot(ctx.sql.clone()).await?;
        let (sync_repo, sync_stop) = IrohSyncRepo::boot(
            Arc::clone(&ctx),
            Arc::clone(&config_repo),
            Arc::clone(&blobs_repo),
            Arc::clone(&doc_blobs_index_repo),
            Some(Arc::clone(&progress_repo)),
        )
        .await?;

        Ok(CliSyncNode {
            ctx,
            drawer: drawer_repo,
            sync_repo,
            sync_stop,
            progress_stop,
            plugs_stop,
            drawer_stop,
            config_stop,
            doc_blobs_index_stop,
            sqlite_local_state_stop,
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cli_clone_and_wait_until_synced_smoke() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let temp_root = std::env::temp_dir().join(format!(
            "daybook-cli-sync-test-{}-{}",
            std::process::id(),
            jiff::Timestamp::now().as_second()
        ));
        let repo_a_path = temp_root.join("repo-a");
        let repo_b_path = temp_root.join("repo-b");

        tokio::fs::create_dir_all(&repo_a_path).await?;
        let init = RepoCtx::init(
            &repo_a_path,
            RepoOpenOptions::default(),
            "cli-test-repo".into(),
            "cli-test-device".into(),
        )
        .await?;
        init.shutdown().await?;

        let node_a = open_cli_sync_node(&repo_a_path).await?;
        for _ in 0..4 {
            node_a
                .drawer
                .add(daybook_types::doc::AddDocArgs {
                    branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                    facets: default(),
                    user_path: Some(daybook_types::doc::UserPathBuf::from(
                        node_a.ctx.local_user_path.clone(),
                    )),
                })
                .await?;
        }
        let ticket = node_a.sync_repo.get_clone_ticket_url().await?;

        cmds::clone::run(ticket.clone(), repo_b_path.to_string_lossy().into_owned()).await?;

        let node_b = open_cli_sync_node(&repo_b_path).await?;
        let bootstrap = node_b.sync_repo.connect_url(&ticket).await?;
        node_b
            .sync_repo
            .wait_until_peers_sync(
                std::slice::from_ref(&big_sync_core::PeerId::new(*bootstrap.id.as_bytes())),
                std::time::Duration::from_secs(120),
            )
            .await?;

        let ids_a = list_doc_ids(&node_a.drawer).await?;
        let ids_b = list_doc_ids(&node_b.drawer).await?;
        assert_eq!(ids_a, ids_b, "cli clone+sync did not converge");

        node_b.stop().await?;
        node_a.stop().await?;
        if let Err(err) = tokio::fs::remove_dir_all(&temp_root).await {
            warn!(?err, path = %temp_root.display(), "failed cleaning test temp root");
        }
        Ok(())
    }
}

