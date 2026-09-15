use crate::interlude::*;

use clap::*;
use daybook_core::drawer::DrawerRepo;
use daybook_types::manifest;

pub struct Ctx {
    clap_cmd: clap::Command,
    command_details: HashMap<String, PlugCmdClap>,
}

impl Ctx {
    pub async fn new() -> Res<Self> {
        let plugs_repo = lazy::plugs_repo().await?;

        let plugs = plugs_repo.list_plugs().await;

        // source plug for each command
        let mut command_details: HashMap<String, PlugCmdClap> = default();
        for plug_man in plugs.iter() {
            let plug_id: Arc<str> = plug_man.id().into();
            for (com_name, com_man) in plug_man.commands.iter() {
                let details =
                    plug_cmd_to_clap(Arc::clone(&plug_id), plug_man, &com_name.0, com_man)?;

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

        Ok(Self {
            clap_cmd: clap::Command::new("exec")
                .visible_alias("x")
                .styles(crate::CLAP_STYLE)
                .subcommands(command_details.values().map(|details| details.clap.clone())),
            command_details,
        })
    }

    pub fn subcmd(&self) -> clap::Command {
        self.clap_cmd.clone()
    }
}

pub async fn run(mut ccx: Ctx, sub_matches: &clap::ArgMatches) -> Res<ExitCode> {
    let cx = lazy::repo_ctx().await?;
    let drawer_repo = lazy::drawer_repo().await?;

    match sub_matches.subcommand() {
        Some((name, sub_matches)) => {
            let details = ccx.command_details.remove(name).unwrap();
            let rt = lazy::daybook_rt().await?;
            let ecx = ExecCtx {
                rt: Arc::clone(&rt),
                _cx: Arc::clone(&cx),
                drawer: Arc::clone(&drawer_repo),
            };

            let res = (details.action)(sub_matches.clone(), ecx).await;

            res?;

            Ok(ExitCode::SUCCESS)
        }
        _ => {
            ccx.clap_cmd.print_long_help()?;
            Ok(ExitCode::FAILURE)
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
        .styles(crate::CLAP_STYLE);

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
Routine acl: {routine_acl:?}
Routine impl: {routine_impl:?}
",
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
                                if !branches.branches.contains_key(val) {
                                    eyre::bail!("branch not found for doc: {doc_id} - {val}");
                                }
                                daybook_types::doc::BranchPathBuf::from(val.as_str())
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
                                daybook_core::rt::DispatchArgs::DocRoutine {
                                    doc_id: doc_id.clone(),
                                    branch_path: branch_path.clone(),
                                    heads: heads.clone(),
                                    invocation:
                                        daybook_core::rt::dispatch::RoutineInvocation::Command,
                                    changed_facet_keys: vec![],
                                    wflow_args_json: None,
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
