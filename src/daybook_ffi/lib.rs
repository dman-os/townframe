#[allow(unused)]
mod interlude {
    pub(crate) use crate::{Ctx, SharedCtx};
    pub use api_utils_rs::prelude::*;
    pub use autosurgeon::{Hydrate, Reconcile};
    pub use std::{
        borrow::Cow,
        collections::HashMap,
        path::{Path, PathBuf},
        rc::Rc,
        sync::{Arc, LazyLock, RwLock},
    };
    pub use utils_rs::{CHeapStr, DHashMap};
}

use crate::interlude::*;
pub use daybook_core::app::SqlCtx;

uniffi::setup_scaffolding!();

mod ffi;
mod macros;
mod repos;

/// Configuration for the daybook core storage systems
#[derive(Debug, Clone)]
pub struct Config {
    pub am: utils_rs::am::Config,
    pub sql: daybook_core::app::SqlConfig,
    pub blobs_root: PathBuf,
}

impl Config {
    /// Create a new config with platform-specific defaults
    pub fn new() -> Res<Self> {
        #[cfg(target_os = "android")]
        let (am, sql, blobs_root) = {
            // On Android, use the app's internal storage directory
            // This will be something like /data/data/org.example.daybook/files/samod
            let app_dir = std::env::var("ANDROID_DATA")
                .map(|data| {
                    PathBuf::from(data)
                        .join("data")
                        .join("org.example.daybook")
                        .join("files")
                })
                .unwrap_or_else(|_| PathBuf::from("/data/data/org.example.daybook/files"));

            (
                utils_rs::am::Config {
                    storage: utils_rs::am::StorageConfig::Disk {
                        path: app_dir.join("samod"),
                    },
                    peer_id: "daybook_client".to_string(),
                },
                daybook_core::app::SqlConfig {
                    database_url: {
                        let db_path = app_dir.join("sqlite.db");
                        format!("sqlite://{}", db_path.display())
                    },
                },
                app_dir.join("blobs"),
            )
        };

        #[cfg(not(target_os = "android"))]
        let (am, sql, blobs_root) = {
            // On desktop platforms, use XDG directories
            let dirs = directories::ProjectDirs::from("org", "daybook", "daybook")
                .ok_or_eyre("failed to get xdg directories")?;
            (
                utils_rs::am::Config {
                    storage: utils_rs::am::StorageConfig::Disk {
                        path: dirs.data_dir().join("samod"),
                    },
                    peer_id: "daybook_client".to_string(),
                },
                daybook_core::app::SqlConfig {
                    database_url: {
                        let db_path = dirs.data_dir().join("sqlite.db");
                        format!("sqlite://{}", db_path.display())
                    },
                },
                dirs.data_dir().join("blobs"),
            )
        };
        Ok(Self {
            am,
            sql,
            blobs_root,
        })
    }
}

struct Ctx {
    config: Config,
    acx: utils_rs::am::AmCtx,
    _acx_stop: tokio::sync::Mutex<Option<utils_rs::am::AmCtxStopToken>>,
    // rt: tokio::runtime::Handle,
    _sql: SqlCtx,
    blobs: Arc<daybook_core::blobs::BlobsRepo>,

    doc_app: tokio::sync::OnceCell<::samod::DocHandle>,
    doc_drawer: tokio::sync::OnceCell<::samod::DocHandle>,

    local_actor_id: automerge::ActorId,
    local_user_path: String,
}

type SharedCtx = Arc<Ctx>;

impl Ctx {
    async fn init(config: Config) -> Result<Arc<Self>, eyre::Report> {
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
        acx.spawn_ws_connector("ws://0.0.0.0:8090".into());

        let blobs = daybook_core::blobs::BlobsRepo::new(config.blobs_root.clone()).await?;

        let doc_app = tokio::sync::OnceCell::new();
        let doc_drawer = tokio::sync::OnceCell::new();

        daybook_core::app::init_from_globals(&acx, &sql.db_pool, &doc_app, &doc_drawer).await?;

        let cx = Arc::new(Ctx {
            config,
            acx,
            _acx_stop: Some(acx_stop).into(),
            _sql: sql,
            blobs,
            doc_app,
            doc_drawer,
            local_actor_id,
            local_user_path,
        });

        Ok(cx)
    }

    fn doc_app(&self) -> &samod::DocHandle {
        self.doc_app.get().expect("ctx was not initialized")
    }

    fn doc_drawer(&self) -> &samod::DocHandle {
        self.doc_drawer.get().expect("ctx was not initialized")
    }
}

fn init_tokio() -> Res<tokio::runtime::Runtime> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("error making tokio rt")?;
    Ok(rt)
}
