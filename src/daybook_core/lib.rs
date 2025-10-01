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

/// Configuration for the daybook core storage systems
#[derive(Debug, Clone)]
pub struct Config {
    pub am: AmConfig,
    pub sql: SqlConfig,
}

/// Configuration for Automerge storage
#[derive(Debug, Clone)]
pub struct AmConfig {
    /// Storage directory for Automerge documents
    pub storage_dir: PathBuf,
    /// Peer ID for this client
    pub peer_id: String,
}

/// Configuration for SQLite storage
#[derive(Debug, Clone)]
pub struct SqlConfig {
    /// SQLite database URL
    pub database_url: String,
}

impl Config {
    /// Create a new config with platform-specific defaults
    pub fn new() -> Res<Self> {
        Ok(Self {
            am: AmConfig::default()?,
            sql: SqlConfig::default()?,
        })
    }

    /// Create a config with custom paths
    pub fn with_paths(am_storage_dir: PathBuf, sql_database_url: String) -> Self {
        Self {
            am: AmConfig {
                storage_dir: am_storage_dir,
                peer_id: "daybook_client".to_string(),
            },
            sql: SqlConfig {
                database_url: sql_database_url,
            },
        }
    }
}

impl AmConfig {
    /// Create default AM config with platform-specific storage directory
    fn default() -> Res<Self> {
        let storage_dir = get_default_am_storage_dir()?;
        Ok(Self {
            storage_dir,
            peer_id: "daybook_client".to_string(),
        })
    }
}

impl SqlConfig {
    /// Create default SQL config with platform-specific database path
    fn default() -> Res<Self> {
        let database_url = get_default_sql_database_url()?;
        Ok(Self {
            database_url,
        })
    }
}

/// Get the default Automerge storage directory for the current platform
fn get_default_am_storage_dir() -> Res<PathBuf> {
    #[cfg(target_os = "android")]
    {
        // On Android, use the app's internal storage directory
        // This will be something like /data/data/org.example.daybook/files/samod
        let app_dir = std::env::var("ANDROID_DATA")
            .map(|data| PathBuf::from(data).join("data").join("org.example.daybook").join("files"))
            .unwrap_or_else(|_| PathBuf::from("/data/data/org.example.daybook/files"));
        
        Ok(app_dir.join("samod"))
    }
    
    #[cfg(not(target_os = "android"))]
    {
        // On desktop platforms, use XDG directories
        let dirs = directories::ProjectDirs::from("org", "daybook", "daybook")
            .wrap_err("failed to get project directories")?;
        
        Ok(dirs.data_dir().join("samod"))
    }
}

/// Get the default SQLite database URL for the current platform
fn get_default_sql_database_url() -> Res<String> {
    #[cfg(target_os = "android")]
    {
        // On Android, use the app's internal storage directory
        let app_dir = std::env::var("ANDROID_DATA")
            .map(|data| PathBuf::from(data).join("data").join("org.example.daybook").join("files"))
            .unwrap_or_else(|_| PathBuf::from("/data/data/org.example.daybook/files"));
        
        let db_path = app_dir.join("daybook.db");
        Ok(format!("sqlite://{}", db_path.display()))
    }
    
    #[cfg(not(target_os = "android"))]
    {
        // On desktop platforms, use XDG directories
        let dirs = directories::ProjectDirs::from("org", "daybook", "daybook")
            .wrap_err("failed to get project directories")?;
        
        let db_path = dirs.data_dir().join("daybook.db");
        Ok(format!("sqlite://{}", db_path.display()))
    }
}

mod am;
mod docs;
mod ffi;
mod globals;
mod macros;
mod repos;
mod samod;
mod sql;
mod tables;

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
