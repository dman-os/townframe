#[allow(unused)]
mod interlude {
    pub(crate) use crate::{Ctx, SharedCtx};
    pub use autosurgeon::{Hydrate, Reconcile};
    pub use std::{
        borrow::Cow,
        collections::HashMap,
        path::{Path, PathBuf},
        rc::Rc,
        sync::{Arc, LazyLock, RwLock},
    };
    pub use struct_patch::Patch;
    pub use utils_rs::prelude::*;
    pub use utils_rs::{CHeapStr, DHashMap};
}

use interlude::*;

uniffi::setup_scaffolding!();

// Re-export scaffolding for UniFFI components so their symbols are exported from this
// shared library at link-time. This helps when composing multiple UniFFI components
// into a single crate.
daybook_types::uniffi_reexport_scaffolding!();

mod am;
mod docs;
mod ffi;
mod globals;
mod macros;
mod repos;
mod samod;
mod sql;
mod tables;

/// Configuration for the daybook core storage systems
#[derive(Debug, Clone)]
pub struct Config {
    pub am: am::Config,
    pub sql: sql::Config,
}

impl Config {
    /// Create a new config with platform-specific defaults
    pub fn new() -> Res<Self> {
        #[cfg(target_os = "android")]
        let sql = {
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

            sql::Config {
                database_url: {
                    let db_path = app_dir.join("sqlite.db");
                    format!("sqlite://{}", db_path.display())
                },
            }
        };

        #[cfg(not(target_os = "android"))]
        let sql = {
            // On desktop platforms, use XDG directories
            let dirs = directories::ProjectDirs::from("org", "daybook", "daybook")
                .ok_or_eyre("failed to get xdg directories")?;
            sql::Config {
                database_url: {
                    let db_path = dirs.data_dir().join("sqlite.db");
                    format!("sqlite://{}", db_path.display())
                },
            }
        };
        Ok(Self { 
            am: am::Config {
                storage_dir: PathBuf::from("/tmp/daybook"),
                peer_id: "daybook_client".to_string(),
            },
            sql 
        })
    }
}

struct Ctx {
    config: Config,
    acx: am::AmCtx,
    // rt: tokio::runtime::Handle,
    sql: sql::SqlCtx,
}

type SharedCtx = Arc<Ctx>;

impl Ctx {
    async fn new(config: Config) -> Result<Arc<Self>, eyre::Report> {
        let sql = sql::SqlCtx::new(config.sql.clone()).await?;
        let acx = am::AmCtx::new(config.am.clone()).await?;
        let cx = Arc::new(Self {
            config,
            acx,
            // rt: tokio::runtime::Handle::current(),
            sql,
        });
        // Initialize automerge document from globals/kv and start sync worker lazily.
        cx.acx.init_from_globals(cx.clone()).await?;
        Ok(cx)
    }
}

fn init_tokio() -> Res<tokio::runtime::Runtime> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .wrap_err("error making tokio rt")?;
    Ok(rt)
}
