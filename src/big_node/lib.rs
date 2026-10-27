use big_cabinet::Authority;
use big_cabinet::DocAccess;
use big_cabinet::DocsConfig;
use big_cabinet::DocsRepo;
use big_cabinet::DrawerAccess;
use big_cabinet::DrawerConfig;
use big_cabinet::Drawers;
use big_cabinet::SchemaRepo;
use big_cabinet::drawer::access::DrawerWorker;
use big_cabinet::index::Indexes;
use big_cabinet_core::authority::Actor;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::Rights;
use big_repo::BigRepo;
use big_repo::BigRepoStopToken;
use big_repo::SharedBigRepo;
use futures::FutureExt;
use secrets_rs::SecretStore;
use service_utils_rs::RequestContext;
use sqlite_rs::Database;
use sqlite_rs::SqliteRuntime;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use utils_rs::prelude::Res;

pub struct Config<'a> {
    pub repo: big_repo::Config,
    pub sql: Database,
    pub sqlite_runtime: Arc<SqliteRuntime>,
    pub schemas: Arc<SchemaRepo>,
    pub secrets: &'a SecretStore,
    pub index_concurrency: NonZeroUsize,
}

pub struct BigNode {
    pub repo: SharedBigRepo,
    pub authority: Authority,
    pub docs_actor: Actor,
    pub docs_namespace: Namespace,
    pub drawers_actor: Actor,
    pub drawers_namespace: Namespace,
    pub indexes: Arc<Indexes>,
    pub drawer_access: Arc<DrawerAccess>,
    pub docs: DocsRepo,
    pub drawers: Drawers,
}

impl BigNode {
    pub async fn boot(config: Config<'_>) -> Res<(Self, StopToken)> {
        let mut stop = StopToken::default();
        let boot: Res<Self> = async {
            let (authority, authority_stop) =
                big_cabinet::authority::boot(config.secrets, config.sql.clone()).await?;
            stop.stages.push(Stage::Service(authority_stop));
            let open = |path: &str| {
                authority.client.call(
                    RequestContext::root(),
                    OpenActor {
                        parent: authority.root,
                        path: path.into(),
                        rights: Rights::all(),
                    },
                )
            };
            let docs_actor = open("documents").await?;
            let drawers_actor = open("drawers").await?;
            let docs_namespace = authority
                .service
                .open_namespace(docs_actor.actor_id)
                .await?;
            let drawers_namespace = authority
                .service
                .open_namespace(drawers_actor.actor_id)
                .await?;

            let (repo, repo_stop) = BigRepo::boot(config.sqlite_runtime, config.repo).await?;
            stop.stages.push(Stage::Repo(repo_stop));
            let (indexes, index_stop) = Indexes::boot(
                Arc::clone(&repo),
                Arc::clone(&config.schemas),
                config.sql.clone(),
                config.index_concurrency,
            )
            .await?;
            stop.stages.push(Stage::Indexes(index_stop));
            let (drawer_access, provider, policy_stop) = DrawerAccess::open(
                Arc::clone(&repo),
                authority.client.clone(),
                drawers_actor.capability,
                drawers_namespace,
                config.sql.clone(),
                Arc::clone(&indexes.facet_set),
            )
            .await?;
            stop.stages.push(Stage::Service(policy_stop));
            let (docs, docs_stop) = big_cabinet::doc::prepare(DocsConfig {
                big_repo: Arc::clone(&repo),
                schema_repo: config.schemas,
                authority: authority.client.clone(),
                issuer: docs_actor.capability,
                namespace: docs_namespace,
                node_issuer: authority.node_issuer.clone(),
                access: Arc::clone(&drawer_access) as Arc<dyn DocAccess>,
                sql: config.sql,
                facet_set: Some(Arc::clone(&indexes.facet_set)),
                facet_ref: Some(Arc::clone(&indexes.facet_ref)),
            })
            .await?;
            stop.stages.push(Stage::Service(docs_stop));
            let (worker, worker_stop) = provider.start(docs.client.clone());
            stop.stages.push(Stage::DrawerWorker {
                worker,
                stop: worker_stop,
            });
            docs.recover().await?;
            let Stage::DrawerWorker { worker, .. } = stop
                .stages
                .last()
                .expect("drawer worker was constructed last")
            else {
                unreachable!("drawer worker was constructed last");
            };
            let (drawers, drawers_stop) =
                big_cabinet::boot_drawers(DrawerConfig { worker }).await?;
            stop.stages.push(Stage::Service(drawers_stop));
            Ok(Self {
                repo,
                authority,
                docs_actor,
                docs_namespace,
                drawers_actor,
                drawers_namespace,
                indexes,
                drawer_access,
                docs,
                drawers,
            })
        }
        .await;
        match boot {
            Ok(node) => Ok((node, stop)),
            Err(error) => match stop.stop().await {
                Ok(()) => Err(error),
                Err(shutdown) => Err(error.wrap_err(format!("boot cleanup failed: {shutdown:#}"))),
            },
        }
    }
}

enum Stage {
    Service(service_utils_rs::StopToken),
    Indexes(big_cabinet::index::StopToken),
    Repo(BigRepoStopToken),
    DrawerWorker {
        worker: DrawerWorker,
        stop: service_utils_rs::StopToken,
    },
}

impl Stage {
    async fn stop(self) -> Res<()> {
        match self {
            Self::Service(stop) => stop.stop().await,
            Self::Indexes(stop) => stop.stop().await,
            Self::Repo(stop) => return stop.stop().await,
            Self::DrawerWorker { worker, stop } => {
                stop.stop().await;
                drop(worker);
            }
        }
        Ok(())
    }
}

#[must_use]
#[derive(Default)]
pub struct StopToken {
    stages: Vec<Stage>,
}

impl StopToken {
    pub async fn stop(mut self) -> Res<()> {
        let mut failure: Option<utils_rs::prelude::eyre::Report> = None;
        let mut panic = None;
        while let Some(stage) = self.stages.pop() {
            match AssertUnwindSafe(stage.stop()).catch_unwind().await {
                Ok(Ok(())) => (),
                Ok(Err(error)) => {
                    failure = Some(match failure {
                        Some(previous) => {
                            previous.wrap_err(format!("additional shutdown failure: {error:#}"))
                        }
                        None => error,
                    })
                }
                Err(payload) => {
                    if panic.is_none() {
                        panic = Some(payload);
                    }
                }
            }
        }
        if let Some(panic) = panic {
            std::panic::resume_unwind(panic);
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod e2e;
