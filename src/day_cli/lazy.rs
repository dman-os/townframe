use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::OnceLock;

use crate::interlude::*;

use crate::client::NodeClient;
use crate::config::CliConfig;
use crate::context::Config;

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

/// The `--node` flag (or `DAY_NODE`), recorded once argv has parsed. Everything lazy below reads
/// it from here, so the static and the dynamic CLI source one config.
static NODE_FLAG: OnceLock<Option<PathBuf>> = OnceLock::new();

pub fn set_node_flag(node: Option<PathBuf>) {
    assert!(
        NODE_FLAG.set(node).is_ok(),
        "argv is parsed into the node flag once"
    );
}

pub async fn cli_config() -> Res<Arc<CliConfig>> {
    static CONFIG: tokio::sync::OnceCell<Arc<CliConfig>> = tokio::sync::OnceCell::const_new();
    CONFIG
        .get_or_try_init(|| async {
            let node_flag = NODE_FLAG
                .get()
                .expect("argv is parsed before config is read");
            let conf = CliConfig::source(node_flag.as_deref())?;
            debug!(?conf, "config sourced");
            eyre::Ok(Arc::new(conf))
        })
        .await
        .map(Arc::clone)
}

pub async fn config() -> Res<Arc<Config>> {
    static CONFIG: tokio::sync::OnceCell<Arc<Config>> = tokio::sync::OnceCell::const_new();
    CONFIG
        .get_or_try_init(|| async {
            let cli_config = cli_config().await?;
            eyre::Ok(Arc::new(Config::new(cli_config)?))
        })
        .await
        .map(Arc::clone)
}

/// The client for the node: its daemon if one is serving, otherwise a node booted in this
/// process. The shutdown callback owns the client and stops it at [`shutdown`]; commands hold
/// clones only while they run.
pub async fn node() -> Res<Arc<NodeClient>> {
    static NODE: tokio::sync::OnceCell<std::sync::Weak<NodeClient>> =
        tokio::sync::OnceCell::const_new();
    let owner = NODE
        .get_or_try_init(|| async {
            let conf = config().await?;
            let client = Arc::new(NodeClient::connect(&conf.node_dir).await?);
            let weak = Arc::downgrade(&client);
            register_shutdown(move || async move {
                Arc::try_unwrap(client)
                    .unwrap_or_else(|_| panic!("a command still holds the node client at shutdown"))
                    .stop()
                    .await
            });
            eyre::Ok(weak)
        })
        .await?;
    Ok(owner
        .upgrade()
        .expect("the node client lives until shutdown"))
}
