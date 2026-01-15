use crate::interlude::*;
pub use daybook_core::app::SqlCtx;

/// Configuration for the daybook core storage systems
#[derive(Debug, Clone)]
pub struct Config {
    pub am: utils_rs::am::Config,
    pub sql: SqlConfig,
    pub _blobs_root: PathBuf,
    pub cli_config: Arc<crate::config::CliConfig>,
}

#[derive(Debug, Clone)]
pub struct SqlConfig {
    pub database_url: String,
}

pub struct Ctx {
    pub acx: utils_rs::am::AmCtx,
    pub acx_stop: tokio::sync::Mutex<Option<utils_rs::am::AmCtxStopToken>>,
    pub _sql: SqlCtx,
    pub doc_app: tokio::sync::OnceCell<samod::DocHandle>,
    pub doc_drawer: tokio::sync::OnceCell<samod::DocHandle>,
    pub local_actor_id: automerge::ActorId,
    pub local_user_path: String,
}

pub type SharedCtx = Arc<Ctx>;

impl Config {
    pub async fn is_repo_initialized(&self) -> Res<bool> {
        Ok(utils_rs::file_exists(&self.cli_config.repo_path.join("db.repo.txt")).await?)
        //&& utils_rs::file_exists(&self.cli_config.repo_path.join("sqlite.db")).await?
    }
    /// Create a new config with platform-specific defaults
    pub fn new(cli_config: Arc<crate::config::CliConfig>) -> Res<Self> {
        let (am, sql, blobs_root) = {
            (
                utils_rs::am::Config {
                    storage: utils_rs::am::StorageConfig::Disk {
                        path: cli_config.repo_path.join("samod"),
                    },
                    peer_id: "daybook_client".to_string(),
                },
                SqlConfig {
                    database_url: {
                        let db_path = cli_config.repo_path.join("sqlite.db");
                        format!("sqlite://{}", db_path.display())
                    },
                },
                cli_config.repo_path.join("blobs"),
            )
        };
        Ok(Self {
            am,
            sql,
            cli_config,
            _blobs_root: blobs_root,
        })
    }
}

impl Ctx {
    pub async fn init(config: Arc<Config>) -> Result<Arc<Self>, eyre::Report> {
        let sql = SqlCtx::new(&config.sql.database_url).await?;

        // Load local identity from SQL
        let local_user_path = daybook_core::app::get_local_user_path(&sql.db_pool).await?;
        let local_user_path = match local_user_path {
            Some(path) => path,
            None => {
                let path = "/default-device".to_string();
                daybook_core::app::set_local_user_path(&sql.db_pool, &path).await?;
                path
            }
        };
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(
            &daybook_types::doc::UserPath::from(local_user_path.clone()),
        );

        let (acx, acx_stop) =
            utils_rs::am::AmCtx::boot(config.am.clone(), Option::<samod::AlwaysAnnounce>::None)
                .await?;
        // acx.spawn_ws_connector("ws://0.0.0.0:8090".into());

        let doc_app = tokio::sync::OnceCell::new();
        let doc_drawer = tokio::sync::OnceCell::new();

        daybook_core::app::init_from_globals(&acx, &sql.db_pool, &doc_app, &doc_drawer).await?;

        let cx = Arc::new(Self {
            acx,
            acx_stop: Some(acx_stop).into(),
            _sql: sql,
            doc_app,
            doc_drawer,
            local_actor_id,
            local_user_path,
        });
        Ok(cx)
    }

    pub fn doc_app(&self) -> &samod::DocHandle {
        self.doc_app.get().expect("ctx was not initialized")
    }

    pub fn doc_drawer(&self) -> &samod::DocHandle {
        self.doc_drawer.get().expect("ctx was not initialized")
    }
}
