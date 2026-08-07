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
use daybook_core::rt::init::InitRepo;
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
                    Arc::clone(&ctx.part_store),
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
                ctx.doc_app.document_id(),
                daybook_types::doc::UserPathBuf::from(ctx.local_user_path.clone()),
            )
            .await?;
            plugs.ensure_system_plugs().await?;
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
                Arc::clone(&ctx.part_store),
                ctx.doc_drawer.document_id(),
                ctx.local_user_path.clone(),
                ctx.sql.clone(),
                ctx.layout.repo_root.join("local_state"),
                Arc::new(surelock::mutex::Mutex::new(
                    daybook_core::drawer::lru::KeyedLruPool::new(1000),
                )),
                Arc::new(surelock::mutex::Mutex::new(
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
    static CONFIG_REPO: tokio::sync::OnceCell<Arc<ConfigRepo>> = tokio::sync::OnceCell::const_new();
    match CONFIG_REPO
        .get_or_try_init(|| async {
            let ctx = repo_ctx().await?;
            let plugs = plugs_repo().await?;
            let (config_repo, config_stop) = ConfigRepo::load(
                Arc::clone(&ctx.big_repo),
                ctx.doc_app.document_id(),
                Arc::clone(&plugs),
                daybook_types::doc::UserPathBuf::from(ctx.local_user_path.clone()),
                ctx.sql.clone(),
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
    static DISPATCH: tokio::sync::OnceCell<Arc<DispatchRepo>> = tokio::sync::OnceCell::const_new();
    match DISPATCH
        .get_or_try_init(|| async {
            let ctx = repo_ctx().await?;
            let (dispatch, dispatch_stop) = DispatchRepo::load(
                Arc::clone(&ctx.big_repo),
                ctx.doc_app.document_id(),
                daybook_types::doc::UserPathBuf::from(ctx.local_user_path.clone()),
                ctx.sql.clone(),
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
    static PROGRESS: tokio::sync::OnceCell<Arc<ProgressRepo>> = tokio::sync::OnceCell::const_new();
    match PROGRESS
        .get_or_try_init(|| async {
            let ctx = repo_ctx().await?;
            let (repo, stop) = ProgressRepo::boot(ctx.sql.clone()).await?;
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

pub async fn init_repo() -> Res<Arc<InitRepo>> {
    static SYNC: tokio::sync::OnceCell<Arc<InitRepo>> = tokio::sync::OnceCell::const_new();
    match SYNC
        .get_or_try_init(|| async {
            let rcx = repo_ctx().await?;
            let progress = progress_repo().await?;
            let (repo, stop) = InitRepo::load(
                Arc::clone(&rcx.big_repo),
                rcx.doc_app.document_id(),
                rcx.local_user_path.clone(),
                rcx.sql.clone(),
                Arc::clone(&progress),
                None,
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
            let init_repo = init_repo().await?;
            let local_state_sqlite_repo = sqlite_local_state_repo().await?;
            let (rt, stop) = daybook_core::rt::Rt::boot(
                daybook_core::rt::RtConfig {
                    device_id: "main_todo".into(),
                    startup_progress_task_id: None,
                },
                Arc::clone(&ctx),
                Arc::clone(&drawer),
                Arc::clone(&plugs),
                Arc::clone(&dispatch),
                Arc::clone(&progress),
                Arc::clone(&blobs),
                Arc::clone(&config_repo),
                Arc::clone(&init_repo),
                Arc::clone(&local_state_sqlite_repo),
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
