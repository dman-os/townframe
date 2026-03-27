//! FIXME: use ctrl_c handlers aross major await points
//! FIXME: make each command a submodule

#[allow(unused)]
mod interlude {
    pub use am_utils_rs::prelude::*;
    pub use utils_rs::prelude::*;

    pub use crate::context::SharedCtx;
}

use crate::interlude::*;

use std::process::ExitCode;

use clap::builder::styling::AnsiColor;
use clap::*;

use daybook_core::drawer::DrawerRepo;
use daybook_core::repos::Repo;
use daybook_core::sync::IrohSyncEvent;
use daybook_types::manifest;

mod config;
mod context;

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

    let res = lazy::rt().block_on(dynamic_cli(static_res));
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
        let ctx = context::open_repo_ctx(&conf, true).await?;
        ctx.shutdown().await?;
        info!(
            path = ?conf.cli_config.repo_path,
            "repo initialization success"
        );
        return Ok(ExitCode::SUCCESS);
    }

    if let StaticCommands::Clone {
        source,
        destination,
    } = &cli.command
    {
        clone_repo_from_url(source, &std::path::PathBuf::from(destination)).await?;
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
    let ctx = lazy::repo_ctx().await?;
    let drawer_repo = lazy::drawer_repo().await?;

    match cli.command {
        StaticCommands::Init {}
        | StaticCommands::Clone { .. }
        | StaticCommands::Completions { .. } => unreachable!(),
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
                            *val = serde_json::Value::String(format!(
                                "byte array, len = {}",
                                values.len()
                            ))
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
            let doc_entries = drawer_repo.list().await?;
            let mut docs = Vec::new();
            for entry in &doc_entries {
                let Some(main_branch) = entry.main_branch_path() else {
                    warn!(doc_id = ?entry.doc_id,"no branches found on doc");
                    continue;
                };
                if let Some(doc) = drawer_repo
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
            let Ok(Some(branches)) = drawer_repo.get_doc_branches(&id).await else {
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
            let doc = drawer_repo
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
            let id = drawer_repo.add(doc).await?;
            info!(id, "created document");
            println!("{id}");
        }
        StaticCommands::Ed { id, branch } => {
            let Ok(Some(branches)) = drawer_repo.get_doc_branches(&id).await else {
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
            let Some((doc, heads)) = drawer_repo.get_with_heads(&id, &branch_path, None).await?
            else {
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
                drawer_repo
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
                Arc::clone(&drawer_repo),
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
        StaticCommands::Sync {
            sync_urls,
            exit_when_synced,
        } => {
            let sync_repo = lazy::sync_repo().await?;
            let local_ticket_url = sync_repo.get_ticket_url().await?;
            {
                use qrcode::render::unicode;
                use qrcode::QrCode;
                let code = QrCode::new(&local_ticket_url[..]).unwrap();
                let image = code
                    .render::<unicode::Dense1x2>()
                    .dark_color(unicode::Dense1x2::Light)
                    .light_color(unicode::Dense1x2::Dark)
                    .build();
                println!("Scan the following QR code to clone this repo");
                println!();
                println!("{image}");
                println!();
                println!("Or copy the following ticket:");
                println!();
                println!();
                println!("{local_ticket_url}");
                println!();
                println!();
            }

            let mut endpoint_ids = Vec::with_capacity(sync_urls.len());
            for sync_url in &sync_urls {
                let bootstrap = sync_repo.connect_url(sync_url).await?;
                endpoint_ids.push(bootstrap.endpoint_id);
            }

            if exit_when_synced {
                if endpoint_ids.is_empty() {
                    error!("--exit-when-synced requires at least one sync URL");
                    return Ok(ExitCode::FAILURE);
                }
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        // no-op
                    }
                    res = sync_repo
                            // TODO: parametrize timeout
                            .wait_for_full_sync(&endpoint_ids, std::time::Duration::from_secs(30)) => {
                        res?;
                    }
                }
            } else {
                let listener = sync_repo.subscribe(daybook_core::repos::SubscribeOpts::new(512));
                sync_repo.connect_known_devices_once().await?;
                loop {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {
                            break;
                        }
                        event = listener.recv_lossy_async() => {
                            match event {
                                Ok(event) => {
                                    match &*event {
                                        IrohSyncEvent::IncomingConnection {
                                            endpoint_id,
                                            conn_id,
                                            peer_id,
                                        } => {
                                            info!(?endpoint_id, ?conn_id, ?peer_id, "incoming connection");
                                        }
                                        IrohSyncEvent::OutgoingConnection {
                                            endpoint_id,
                                            conn_id,
                                            peer_id,
                                        } => {
                                            info!(?endpoint_id, ?conn_id, ?peer_id, "outgoing connection");
                                        }
                                        IrohSyncEvent::ConnectionClosed { endpoint_id, reason } => {
                                            info!(?endpoint_id, ?reason, "connection closed");
                                        }
                                        IrohSyncEvent::PeerFullySynced {
                                            endpoint_id,
                                            doc_count,
                                        } => {
                                            info!(?endpoint_id, ?doc_count, "peer fully synced");
                                        }
                                        IrohSyncEvent::DocSyncedWithPeer {
                                            endpoint_id,
                                            doc_id,
                                        } => {
                                            info!(?endpoint_id, ?doc_id, "doc synced with peer");
                                        }
                                        IrohSyncEvent::BlobSynced { hash, endpoint_id } => {
                                            info!(?endpoint_id, %hash, "blob synced");
                                        }
                                        IrohSyncEvent::BlobSyncBackoff {
                                            hash,
                                            delay,
                                            attempt_no,
                                        } => {
                                            info!(%hash, ?delay, ?attempt_no, "blob sync backoff");
                                        }
                                        IrohSyncEvent::BlobDownloadStarted {
                                            endpoint_id,
                                            partition,
                                            hash,
                                        } => {
                                            info!(?endpoint_id, ?partition, %hash, "blob download started");
                                        }
                                        IrohSyncEvent::BlobDownloadFinished {
                                            endpoint_id,
                                            partition,
                                            hash,
                                            success,
                                        } => {
                                            info!(?endpoint_id, ?partition, %hash, ?success, "blob download finished");
                                        }
                                        IrohSyncEvent::StalePeer { endpoint_id } => {
                                            warn!(?endpoint_id, "stale sync peer");
                                        }
                                    }
                                }
                                Err(err) => {
                                    warn!(?err, "sync listener closed");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        StaticCommands::Devices { command } => {
            let config_repo = lazy::config_repo().await?;
            match command {
                DevicesCommands::Ls => {
                    use comfy_table::presets::NOTHING;
                    use comfy_table::Table;

                    let mut devices = config_repo.list_known_sync_devices().await?;
                    devices.sort_by_key(|device| device.added_at);

                    let mut table = Table::new();
                    table
                        .load_preset(NOTHING)
                        .set_header(vec!["Endpoint", "Name", "Added At"]);
                    for device in devices {
                        table.add_row(vec![
                            utils_rs::hash::encode_base58_multibase(device.endpoint_id),
                            device.name,
                            device.added_at.to_string(),
                        ]);
                    }
                    println!("{table}");
                }
                DevicesCommands::Add {
                    iroh_ticket_url,
                    name,
                } => {
                    let requester_endpoint_id = Some(ctx.iroh_public_key.clone());
                    let requester_peer_key =
                        Some(format!("/{}/{}", ctx.repo_id, ctx.iroh_public_key));
                    let bootstrap = daybook_core::sync::request_clone_provision_via_rpc(
                        &iroh_ticket_url,
                        daybook_core::sync::CloneProvisionRequest {
                            requested_device_name: None,
                            provision: false,
                            requester_endpoint_id,
                            requester_peer_key,
                        },
                    )
                    .await?
                    .to_bootstrap_state()?;
                    let local_repo_id = ctx.repo_id.clone();
                    if bootstrap.repo_id != local_repo_id {
                        eyre::bail!(
                            "ticket repo_id mismatch (local={}, remote={})",
                            local_repo_id,
                            bootstrap.repo_id
                        );
                    }
                    let device_name = if let Some(name) = name {
                        name.clone()
                    } else if let Some(name) = bootstrap.device_name {
                        name
                    } else {
                        bootstrap.endpoint_id.to_string()
                    };
                    config_repo
                        .upsert_known_sync_device(daybook_core::app::globals::SyncDeviceEntry {
                            endpoint_id: bootstrap.endpoint_id,
                            name: device_name,
                            added_at: Timestamp::now(),
                            last_connected_at: None,
                        })
                        .await?;
                }
            }
        }
    }

    Ok(ExitCode::SUCCESS)
}

async fn clone_repo_from_url(source_url: &str, destination: &std::path::Path) -> Res<()> {
    let res = daybook_core::sync::clone_repo_init_from_url(
        source_url,
        destination,
        daybook_core::sync::CloneRepoInitOptions {
            timeout: std::time::Duration::from_secs(30),
        },
    )
    .await?;
    println!(
        "clone initialization completed at {}",
        res.repo_path.display()
    );
    println!(
        "required clone partitions synced (repo_id={}, repo_name={})",
        res.bootstrap.repo_id, res.bootstrap.repo_name
    );
    println!("full sync can continue in future sync sessions (run: daybook sync <ticket>)");
    Ok(())
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

    let ctx = Box::pin(lazy::repo_ctx()).await?;
    let drawer = Box::pin(lazy::drawer_repo()).await?;
    let plugs_repo = Box::pin(lazy::plugs_repo()).await?;

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
        | Ok(StaticCommands::Clone { .. })
        | Ok(StaticCommands::Cat { .. })
        | Ok(StaticCommands::Ed { .. })
        | Ok(StaticCommands::Devices { .. })
        | Ok(StaticCommands::Sync { .. })
        | Ok(StaticCommands::Livetree { .. }) => {
            unreachable!("static_cli will prevent these");
        }
    }
    match matches.subcommand() {
        Some(("exec", sub_matches)) => match sub_matches.subcommand() {
            Some((name, sub_matches)) => {
                info!(?name, "XXX");
                let details = command_details.remove(name).unwrap();
                let rt = Box::pin(lazy::daybook_rt()).await?;
                let ecx = ExecCtx {
                    rt: Arc::clone(&rt),
                    _cx: Arc::clone(&ctx),
                    drawer: Arc::clone(&drawer),
                };

                let res = (details.action)(sub_matches.clone(), ecx).await;

                res?;

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
    /// Work with the pauperfuse livetree materialization
    Livetree {
        #[clap(subcommand)]
        command: LivetreeCommands,
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
        command: DevicesCommands,
    },
    /// Generate shell completions
    Completions {
        #[clap(value_enum)]
        shell: clap_complete::Shell,
    },
}

#[derive(Debug, clap::Subcommand)]
enum DevicesCommands {
    /// List known devices
    Ls,
    /// Add a device from a bootstrap URL
    Add {
        /// Clone URL: db+iroh-clone:<endpoint-ticket>
        iroh_ticket_url: String,
        /// Override display name
        #[arg(long)]
        name: Option<String>,
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
                    routine_acl = routine.facet_acl(),
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
                        let heads = branches.branches.get(&branch_path.to_string()).unwrap();

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

#[cfg(test)]
mod tests {
    use super::*;
    use daybook_core::blobs::BlobsRepo;
    use daybook_core::config::ConfigRepo;
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
        doc_blobs_index_stop: daybook_core::index::doc_blobs::DocBlobsIndexStopToken,
        sqlite_local_state_stop: RepoStopToken,
    }

    impl CliSyncNode {
        async fn stop(self) -> Res<()> {
            self.sync_stop.stop().await?;
            self.progress_stop.stop().await?;
            self.doc_blobs_index_stop.stop().await?;
            self.sqlite_local_state_stop.stop().await?;
            self.config_stop.stop().await?;
            self.drawer_stop.stop().await?;
            self.plugs_stop.stop().await?;
            self.ctx.shutdown().await?;
            Ok(())
        }
    }

    async fn list_doc_ids(drawer: &DrawerRepo) -> Res<HashSet<String>> {
        let (_, ids) = drawer.list_just_ids().await?;
        Ok(ids.into_iter().collect())
    }

    async fn open_cli_sync_node(repo_root: &std::path::Path) -> Res<CliSyncNode> {
        let ctx =
            Arc::new(RepoCtx::open(repo_root, RepoOpenOptions {}, "cli-test-device".into()).await?);
        let blobs_repo = BlobsRepo::new(
            ctx.layout.blobs_root.clone(),
            ctx.local_user_path.clone(),
            Arc::new(daybook_core::blobs::PartitionStoreMembershipWriter::new(
                ctx.big_repo.partition_store(),
            )),
        )
        .await?;
        let (plugs_repo, plugs_stop) = PlugsRepo::load(
            Arc::clone(&ctx.big_repo),
            Arc::clone(&blobs_repo),
            ctx.doc_app.document_id().clone(),
            daybook_types::doc::UserPath::from(ctx.local_user_path.clone()),
        )
        .await?;
        let (drawer_repo, drawer_stop) = DrawerRepo::load(
            Arc::clone(&ctx.big_repo),
            ctx.doc_drawer.document_id().clone(),
            ctx.local_user_path.clone().into(),
            ctx.layout.repo_root.join("local_state"),
            Arc::new(std::sync::Mutex::new(
                daybook_core::drawer::lru::KeyedLruPool::new(1000),
            )),
            Arc::new(std::sync::Mutex::new(
                daybook_core::drawer::lru::KeyedLruPool::new(1000),
            )),
            Arc::clone(&plugs_repo),
        )
        .await?;
        let (config_repo, config_stop) = ConfigRepo::load(
            Arc::clone(&ctx.big_repo),
            ctx.doc_app.document_id().clone(),
            Arc::clone(&plugs_repo),
            daybook_types::doc::UserPath::from(ctx.local_user_path.clone()),
            ctx.sql.db_pool.clone(),
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
        let (progress_repo, progress_stop) = ProgressRepo::boot(ctx.sql.db_pool.clone()).await?;
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
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");
        let temp_root = std::env::temp_dir().join(format!(
            "daybook-cli-sync-test-{}-{}",
            std::process::id(),
            jiff::Timestamp::now().as_second()
        ));
        let repo_a_path = temp_root.join("repo-a");
        let repo_b_path = temp_root.join("repo-b");

        tokio::fs::create_dir_all(&repo_a_path).await?;
        let init =
            RepoCtx::init(&repo_a_path, RepoOpenOptions {}, "cli-test-device".into()).await?;
        init.shutdown().await?;
        drop(init);

        let node_a = open_cli_sync_node(&repo_a_path).await?;
        for _ in 0..4 {
            node_a
                .drawer
                .add(daybook_types::doc::AddDocArgs {
                    branch_path: daybook_types::doc::BranchPath::from("main"),
                    facets: default(),
                    user_path: Some(daybook_types::doc::UserPath::from(
                        node_a.ctx.local_user_path.clone(),
                    )),
                })
                .await?;
        }
        let ticket = node_a.sync_repo.get_ticket_url().await?;

        clone_repo_from_url(&ticket, &repo_b_path).await?;

        let node_b = open_cli_sync_node(&repo_b_path).await?;
        let bootstrap = node_b.sync_repo.connect_url(&ticket).await?;
        node_b
            .sync_repo
            .wait_for_full_sync(
                std::slice::from_ref(&bootstrap.endpoint_id),
                Duration::from_secs(30),
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

mod lazy {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::OnceLock;

    use crate::interlude::*;

    use crate::config::CliConfig;
    use crate::context::*;
    use daybook_core::blobs::BlobsRepo;
    use daybook_core::config::ConfigRepo;
    use daybook_core::drawer::DrawerRepo;
    use daybook_core::index::DocBlobsIndexRepo;
    use daybook_core::local_state::SqliteLocalStateRepo;
    use daybook_core::plugs::PlugsRepo;
    use daybook_core::progress::ProgressRepo;
    use daybook_core::rt::dispatch::DispatchRepo;
    use daybook_core::sync::IrohSyncRepo;

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

    type ShutdownFuture = Pin<Box<dyn Future<Output = Res<()>> + Send + 'static>>;
    type ShutdownCallback = Box<dyn FnOnce() -> ShutdownFuture + Send + 'static>;

    fn shutdown_callbacks() -> &'static Mutex<Vec<ShutdownCallback>> {
        static SHUTDOWN_CALLBACKS: OnceLock<Mutex<Vec<ShutdownCallback>>> = OnceLock::new();
        SHUTDOWN_CALLBACKS.get_or_init(|| Mutex::new(Vec::new()))
    }

    fn register_shutdown<F, Fut>(callback: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Res<()>> + Send + 'static,
    {
        shutdown_callbacks()
            .lock()
            .expect(ERROR_MUTEX)
            .push(Box::new(move || Box::pin(callback())));
    }

    pub async fn shutdown() -> Res<()> {
        let callbacks = std::mem::take(&mut *shutdown_callbacks().lock().expect(ERROR_MUTEX));
        let mut first_err: Option<eyre::Report> = None;
        for callback in callbacks.into_iter().rev() {
            if let Err(err) = callback().await {
                if first_err.is_none() {
                    first_err = Some(err);
                } else {
                    warn!(?err, "shutdown callback failed after first error");
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
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

    pub async fn repo_ctx() -> Res<SharedCtx> {
        static CTX: tokio::sync::OnceCell<SharedCtx> = tokio::sync::OnceCell::const_new();
        match CTX
            .get_or_try_init(|| async {
                let conf = config().await?;
                let ctx = crate::context::open_repo_ctx(&conf, false).await?;
                register_shutdown({
                    let ctx = Arc::clone(&ctx);
                    move || async move { ctx.shutdown().await }
                });
                Ok(ctx)
            })
            .await
        {
            Ok(ctx) => Ok(Arc::clone(ctx)),
            Err(err) => Err(err),
        }
    }

    pub async fn blobs_repo() -> Res<Arc<BlobsRepo>> {
        static BLOBS: tokio::sync::OnceCell<Arc<BlobsRepo>> = tokio::sync::OnceCell::const_new();
        match BLOBS
            .get_or_try_init(|| async {
                let ctx = repo_ctx().await?;
                let blobs = BlobsRepo::new(
                    ctx.layout.blobs_root.clone(),
                    ctx.local_user_path.clone(),
                    Arc::new(daybook_core::blobs::PartitionStoreMembershipWriter::new(
                        ctx.big_repo.partition_store(),
                    )),
                )
                .await?;
                register_shutdown({
                    let blobs = Arc::clone(&blobs);
                    move || async move { blobs.shutdown().await }
                });
                Ok(blobs)
            })
            .await
        {
            Ok(blobs) => Ok(Arc::clone(blobs)),
            Err(err) => Err(err),
        }
    }

    pub async fn plugs_repo() -> Res<Arc<PlugsRepo>> {
        static PLUGS: tokio::sync::OnceCell<Arc<PlugsRepo>> = tokio::sync::OnceCell::const_new();
        match PLUGS
            .get_or_try_init(|| async {
                let ctx = repo_ctx().await?;
                let blobs = blobs_repo().await?;
                let (plugs, plugs_stop) = PlugsRepo::load(
                    Arc::clone(&ctx.big_repo),
                    Arc::clone(&blobs),
                    ctx.doc_app.document_id().clone(),
                    daybook_types::doc::UserPath::from(ctx.local_user_path.clone()),
                )
                .await?;
                register_shutdown(move || async move { plugs_stop.stop().await });
                Ok(plugs)
            })
            .await
        {
            Ok(plugs) => Ok(Arc::clone(plugs)),
            Err(err) => Err(err),
        }
    }

    pub async fn drawer_repo() -> Res<Arc<DrawerRepo>> {
        static DRAWER: tokio::sync::OnceCell<Arc<DrawerRepo>> = tokio::sync::OnceCell::const_new();
        match DRAWER
            .get_or_try_init(|| async {
                let ctx = repo_ctx().await?;
                let plugs = plugs_repo().await?;
                let (drawer, drawer_stop) = DrawerRepo::load(
                    Arc::clone(&ctx.big_repo),
                    ctx.doc_drawer.document_id().clone(),
                    ctx.local_user_path.clone().into(),
                    ctx.layout.repo_root.join("local_state"),
                    Arc::new(std::sync::Mutex::new(
                        daybook_core::drawer::lru::KeyedLruPool::new(1000),
                    )),
                    Arc::new(std::sync::Mutex::new(
                        daybook_core::drawer::lru::KeyedLruPool::new(1000),
                    )),
                    Arc::clone(&plugs),
                )
                .await?;
                register_shutdown(move || async move { drawer_stop.stop().await });
                Ok(drawer)
            })
            .await
        {
            Ok(drawer) => Ok(Arc::clone(drawer)),
            Err(err) => Err(err),
        }
    }

    pub async fn config_repo() -> Res<Arc<ConfigRepo>> {
        static CONFIG_REPO: tokio::sync::OnceCell<Arc<ConfigRepo>> =
            tokio::sync::OnceCell::const_new();
        match CONFIG_REPO
            .get_or_try_init(|| async {
                let ctx = repo_ctx().await?;
                let plugs = plugs_repo().await?;
                let (config_repo, config_stop) = ConfigRepo::load(
                    Arc::clone(&ctx.big_repo),
                    ctx.doc_app.document_id().clone(),
                    Arc::clone(&plugs),
                    daybook_types::doc::UserPath::from(ctx.local_user_path.clone()),
                    ctx.sql.db_pool.clone(),
                )
                .await?;
                register_shutdown(move || async move { config_stop.stop().await });
                Ok(config_repo)
            })
            .await
        {
            Ok(config_repo) => Ok(Arc::clone(config_repo)),
            Err(err) => Err(err),
        }
    }

    pub async fn dispatch_repo() -> Res<Arc<DispatchRepo>> {
        static DISPATCH: tokio::sync::OnceCell<Arc<DispatchRepo>> =
            tokio::sync::OnceCell::const_new();
        match DISPATCH
            .get_or_try_init(|| async {
                let ctx = repo_ctx().await?;
                let (dispatch, dispatch_stop) = DispatchRepo::load(
                    Arc::clone(&ctx.big_repo),
                    ctx.doc_app.document_id().clone(),
                    daybook_types::doc::UserPath::from(ctx.local_user_path.clone()),
                )
                .await?;
                register_shutdown(move || async move { dispatch_stop.stop().await });
                Ok(dispatch)
            })
            .await
        {
            Ok(dispatch) => Ok(Arc::clone(dispatch)),
            Err(err) => Err(err),
        }
    }

    pub async fn sqlite_local_state_repo() -> Res<Arc<SqliteLocalStateRepo>> {
        static SQLITE_LOCAL_STATE: tokio::sync::OnceCell<Arc<SqliteLocalStateRepo>> =
            tokio::sync::OnceCell::const_new();
        match SQLITE_LOCAL_STATE
            .get_or_try_init(|| async {
                let ctx = repo_ctx().await?;
                let (repo, stop) =
                    SqliteLocalStateRepo::boot(ctx.layout.repo_root.join("local_state")).await?;
                register_shutdown(move || async move { stop.stop().await });
                Ok(repo)
            })
            .await
        {
            Ok(repo) => Ok(Arc::clone(repo)),
            Err(err) => Err(err),
        }
    }

    pub async fn doc_blobs_index_repo() -> Res<Arc<DocBlobsIndexRepo>> {
        static DOC_BLOBS_INDEX: tokio::sync::OnceCell<Arc<DocBlobsIndexRepo>> =
            tokio::sync::OnceCell::const_new();
        match DOC_BLOBS_INDEX
            .get_or_try_init(|| async {
                let drawer = drawer_repo().await?;
                let blobs = blobs_repo().await?;
                let sqlite_local_state = sqlite_local_state_repo().await?;
                let (repo, stop) = DocBlobsIndexRepo::boot(
                    Arc::clone(&drawer),
                    Arc::clone(&blobs),
                    Arc::clone(&sqlite_local_state),
                )
                .await?;
                register_shutdown(move || async move { stop.stop().await });
                Ok(repo)
            })
            .await
        {
            Ok(repo) => Ok(Arc::clone(repo)),
            Err(err) => Err(err),
        }
    }

    pub async fn progress_repo() -> Res<Arc<ProgressRepo>> {
        static PROGRESS: tokio::sync::OnceCell<Arc<ProgressRepo>> =
            tokio::sync::OnceCell::const_new();
        match PROGRESS
            .get_or_try_init(|| async {
                let ctx = repo_ctx().await?;
                let (repo, stop) = ProgressRepo::boot(ctx.sql.db_pool.clone()).await?;
                register_shutdown(move || async move { stop.stop().await });
                Ok(repo)
            })
            .await
        {
            Ok(repo) => Ok(Arc::clone(repo)),
            Err(err) => Err(err),
        }
    }

    pub async fn sync_repo() -> Res<Arc<IrohSyncRepo>> {
        static SYNC: tokio::sync::OnceCell<Arc<IrohSyncRepo>> = tokio::sync::OnceCell::const_new();
        match SYNC
            .get_or_try_init(|| async {
                let ctx = repo_ctx().await?;
                let config = config_repo().await?;
                let blobs = blobs_repo().await?;
                let doc_blobs_index = doc_blobs_index_repo().await?;
                let progress = progress_repo().await?;
                let (repo, stop) = IrohSyncRepo::boot(
                    Arc::clone(&ctx),
                    Arc::clone(&config),
                    Arc::clone(&blobs),
                    Arc::clone(&doc_blobs_index),
                    Some(Arc::clone(&progress)),
                )
                .await?;
                register_shutdown(move || async move { stop.stop().await });
                Ok(repo)
            })
            .await
        {
            Ok(repo) => Ok(Arc::clone(repo)),
            Err(err) => Err(err),
        }
    }

    pub async fn daybook_rt() -> Res<Arc<daybook_core::rt::Rt>> {
        static DAYBOOK_RT: tokio::sync::OnceCell<Arc<daybook_core::rt::Rt>> =
            tokio::sync::OnceCell::const_new();
        match DAYBOOK_RT
            .get_or_try_init(|| async {
                let ctx = repo_ctx().await?;
                let drawer = drawer_repo().await?;
                let plugs = plugs_repo().await?;
                let dispatch = dispatch_repo().await?;
                let progress = progress_repo().await?;
                let blobs = blobs_repo().await?;
                let config_repo = config_repo().await?;
                let (rt, stop) = daybook_core::rt::Rt::boot(
                    daybook_core::rt::RtConfig {
                        device_id: "main_TODO_XXX".into(),
                    },
                    ctx.doc_app.document_id().clone(),
                    format!("sqlite://{}", ctx.layout.sqlite_path.display()),
                    Arc::clone(&ctx.big_repo),
                    Arc::clone(&drawer),
                    Arc::clone(&plugs),
                    Arc::clone(&dispatch),
                    Arc::clone(&progress),
                    Arc::clone(&blobs),
                    Arc::clone(&config_repo),
                    ctx.local_actor_id.clone(),
                    ctx.layout.repo_root.join("local_state"),
                )
                .await?;
                register_shutdown(move || async move { stop.stop().await });
                Ok(rt)
            })
            .await
        {
            Ok(rt) => Ok(Arc::clone(rt)),
            Err(err) => Err(err),
        }
    }
}
