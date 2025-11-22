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
    pub use struct_patch::Patch;
    pub use utils_rs::{CHeapStr, DHashMap};
}

use crate::interlude::*;

uniffi::setup_scaffolding!();

mod am;
mod drawer;
mod ffi;
mod globals;
mod macros;
mod sql;
mod tables;

/// Configuration for the daybook core storage systems
#[derive(Debug, Clone)]
pub struct Config {
    pub am: utils_rs::am::Config,
    pub sql: sql::Config,
}

impl Config {
    /// Create a new config with platform-specific defaults
    pub fn new() -> Res<Self> {
        #[cfg(target_os = "android")]
        let (am, sql) = {
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
                    storage_dir: app_dir.join("samod"),

                    peer_id: "daybook_client".to_string(),
                },
                sql::Config {
                    database_url: {
                        let db_path = app_dir.join("sqlite.db");
                        format!("sqlite://{}", db_path.display())
                    },
                },
            )
        };

        #[cfg(not(target_os = "android"))]
        let (am, sql) = {
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
                sql::Config {
                    database_url: {
                        let db_path = dirs.data_dir().join("sqlite.db");
                        format!("sqlite://{}", db_path.display())
                    },
                },
            )
        };
        Ok(Self { am, sql })
    }
}

struct Ctx {
    // config: Config,
    acx: utils_rs::am::AmCtx,
    // rt: tokio::runtime::Handle,
    sql: sql::SqlCtx,

    doc_app: tokio::sync::OnceCell<::samod::DocHandle>,
    doc_drawer: tokio::sync::OnceCell<::samod::DocHandle>,
}

type SharedCtx = Arc<Ctx>;

impl Ctx {
    async fn init(config: Config) -> Result<Arc<Self>, eyre::Report> {
        let sql = sql::SqlCtx::new(config.sql.clone()).await?;
        let acx =
            utils_rs::am::AmCtx::boot(config.am.clone(), Option::<samod::AlwaysAnnounce>::None)
                .await?;
        acx.spawn_ws_connector("ws://0.0.0.0:8090".into());

        let cx = Arc::new(Self {
            // config,
            acx,
            // rt: tokio::runtime::Handle::current(),
            sql,
            doc_app: default(),
            doc_drawer: default(),
        });
        // Initialize automerge document from globals/kv and start sync worker lazily.
        am::init_from_globals(&cx).await?;
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
