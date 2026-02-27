use crate::interlude::*;
use automerge::transaction::Transactable;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::str::FromStr;

pub struct SqlCtx {
    pub db_pool: SqlitePool,
}

#[derive(Debug, Clone)]
pub struct SqlConfig {
    pub database_url: String,
}

impl SqlCtx {
    pub async fn new(database_url: &str) -> Res<Self> {
        if !database_url.starts_with("sqlite::memory:") {
            if let Some(path) = database_url.strip_prefix("sqlite://") {
                if let Some(parent) = std::path::Path::new(path).parent() {
                    std::fs::create_dir_all(parent).wrap_err_with(|| {
                        format!("Failed to create database directory: {}", parent.display())
                    })?;
                }
            }
        }

        let db_pool = SqlitePoolOptions::new()
            .connect_with(
                SqliteConnectOptions::from_str(database_url)?
                    .journal_mode(SqliteJournalMode::Wal)
                    .busy_timeout(std::time::Duration::from_secs(5))
                    .create_if_missing(true),
            )
            .await
            .wrap_err("error initializing sqlite db")?;

        sqlx::query(
            r#"
                CREATE TABLE IF NOT EXISTS kvstore (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                "#,
        )
        .execute(&db_pool)
        .await?;

        Ok(Self { db_pool })
    }
}

#[derive(Debug, Clone)]
pub struct GlobalConfig {
    pub app_data_dir: std::path::PathBuf,
    pub sql: SqlConfig,
    pub default_repo_root: std::path::PathBuf,
}

impl GlobalConfig {
    pub fn new() -> Res<Self> {
        let app_data_dir = app_data_dir()?;
        let sql = SqlConfig {
            database_url: format!(
                "sqlite://{}",
                app_data_dir.join("globals.sqlite.db").display()
            ),
        };
        Ok(Self {
            app_data_dir: app_data_dir.clone(),
            sql,
            default_repo_root: app_data_dir.join("repo"),
        })
    }
}

fn app_data_dir() -> Res<std::path::PathBuf> {
    #[cfg(target_os = "android")]
    {
        let app_dir = std::env::var("ANDROID_DATA")
            .map(|data| {
                std::path::PathBuf::from(data)
                    .join("data")
                    .join("org.example.daybook")
                    .join("files")
            })
            .unwrap_or_else(|_| std::path::PathBuf::from("/data/data/org.example.daybook/files"));
        Ok(app_dir)
    }
    #[cfg(not(target_os = "android"))]
    {
        let dirs = directories::ProjectDirs::from("org", "daybook", "daybook")
            .ok_or_eyre("failed to get xdg directories")?;
        Ok(dirs.data_dir().into())
    }
}

pub struct GlobalCtx {
    pub config: GlobalConfig,
    pub sql: SqlCtx,
}

impl GlobalCtx {
    pub async fn new() -> Res<Self> {
        let config = GlobalConfig::new()?;
        let sql = SqlCtx::new(&config.sql.database_url).await?;
        Ok(Self { config, sql })
    }
}

pub mod version_updates {
    use crate::interlude::*;

    use automerge::{transaction::Transactable, ActorId, AutoCommit, ROOT};
    use autosurgeon::reconcile_prop;

    use crate::config::ConfigStore;
    use crate::plugs::PlugsStore;
    use crate::rt::dispatch::DispatchStore;
    use crate::rt::switch::SwitchStateStore;
    use crate::tables::TablesStore;

    pub fn version_latest() -> Res<Vec<u8>> {
        use crate::stores::Store;
        let mut doc = AutoCommit::new().with_actor(ActorId::random());
        doc.put(ROOT, "version", "0")?;
        // annotate schema for app document
        doc.put(ROOT, "$schema", "daybook.app")?;
        reconcile_prop(
            &mut doc,
            ROOT,
            TablesStore::prop().as_ref(),
            TablesStore::default(),
        )?;
        reconcile_prop(
            &mut doc,
            ROOT,
            ConfigStore::prop().as_ref(),
            ConfigStore::default(),
        )?;
        reconcile_prop(
            &mut doc,
            ROOT,
            PlugsStore::prop().as_ref(),
            PlugsStore::default(),
        )?;
        reconcile_prop(
            &mut doc,
            ROOT,
            DispatchStore::prop().as_ref(),
            DispatchStore::default(),
        )?;
        reconcile_prop(
            &mut doc,
            ROOT,
            SwitchStateStore::prop().as_ref(),
            SwitchStateStore::default(),
        )?;
        Ok(doc.save_nocompress())
    }
}

pub async fn init_from_globals(
    acx: &AmCtx,
    sql: &SqlitePool,
    doc_app_cell: &tokio::sync::OnceCell<samod::DocHandle>,
    doc_drawer_cell: &tokio::sync::OnceCell<samod::DocHandle>,
) -> Res<()> {
    let init_state = globals::get_init_state(sql).await?;
    let (handle_app, handle_drawer) = if let globals::InitState::Created {
        doc_id_app,
        doc_id_drawer,
    } = init_state
    {
        let (handle_app, handle_drawer) =
            tokio::try_join!(acx.find_doc(&doc_id_app), acx.find_doc(&doc_id_drawer))?;
        if handle_app.is_none() {
            warn!("doc not found locally for stored doc_id_app; creating new local document");
        }
        if handle_drawer.is_none() {
            warn!("doc not found locally for stored doc_id_drawer; creating new local document");
        }
        (handle_app, handle_drawer)
    } else {
        (None, None)
    };

    let mut doc_handles = vec![];
    let mut update_state = false;
    for (handle, latest_fn) in [
        (
            handle_app,
            version_updates::version_latest as fn() -> Res<Vec<u8>>,
        ),
        (
            handle_drawer,
            (|| {
                let mut doc = automerge::AutoCommit::new();
                doc.put(automerge::ROOT, "version", "0")?;
                Ok(doc.save_nocompress())
            }) as fn() -> Res<Vec<u8>>,
        ),
    ] {
        let handle = match handle {
            Some(handle) => handle,
            None => {
                update_state = true;
                let doc = latest_fn()?;
                let doc =
                    automerge::Automerge::load(&doc).wrap_err("error loading version_latest")?;
                let handle = acx.add_doc(doc).await?;
                handle
            }
        };
        doc_handles.push(handle)
    }
    if doc_handles.len() != 2 {
        unreachable!();
    }
    for handle in &doc_handles {
        let _ = acx.change_manager().add_doc(handle.clone()).await?;
    }
    if update_state {
        globals::set_init_state(
            sql,
            &globals::InitState::Created {
                doc_id_app: doc_handles[0].document_id().clone(),
                doc_id_drawer: doc_handles[1].document_id().clone(),
            },
        )
        .await?;
    }
    let (Ok(()), Ok(())) = (
        doc_drawer_cell.set(doc_handles.pop().unwrap_or_log()),
        doc_app_cell.set(doc_handles.pop().unwrap_or_log()),
    ) else {
        eyre::bail!("double ctx initialization");
    };
    Ok(())
}

pub mod globals {
    use std::time::{SystemTime, UNIX_EPOCH};

    use sqlx::SqlitePool;

    use crate::interlude::*;

    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
    pub enum InitState {
        None,
        Created {
            doc_id_app: DocumentId,
            doc_id_drawer: DocumentId,
        },
    }
    const INIT_STATE_KEY: &str = "init_state";

    pub async fn get_init_state(sql: &SqlitePool) -> Res<InitState> {
        let rec = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
            .bind(INIT_STATE_KEY)
            .fetch_optional(sql)
            .await?;
        let state = match rec {
            Some(json) => serde_json::from_str::<InitState>(&json)?,
            None => InitState::None,
        };
        Ok(state)
    }
    pub async fn set_init_state(sql: &SqlitePool, state: &InitState) -> Res<()> {
        let json = serde_json::to_string(state)?;
        sqlx::query("INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
        .bind(INIT_STATE_KEY)
        .bind(&json)
        .execute(sql)
        .await?;
        Ok(())
    }

    const LOCAL_USER_PATH_KEY: &str = "local_user_path";
    pub async fn get_local_user_path(sql: &SqlitePool) -> Res<Option<String>> {
        let rec = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
            .bind(LOCAL_USER_PATH_KEY)
            .fetch_optional(sql)
            .await?;
        Ok(rec)
    }

    pub async fn set_local_user_path(sql: &SqlitePool, path: &str) -> Res<()> {
        sqlx::query("INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
        .bind(LOCAL_USER_PATH_KEY)
        .bind(path)
        .execute(sql)
        .await?;
        Ok(())
    }

    const REPO_ID_KEY: &str = "repo_id";

    pub async fn get_repo_id(sql: &SqlitePool) -> Res<Option<String>> {
        let rec = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
            .bind(REPO_ID_KEY)
            .fetch_optional(sql)
            .await?;
        Ok(rec)
    }

    pub async fn set_repo_id(sql: &SqlitePool, repo_id: &str) -> Res<()> {
        sqlx::query("INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
            .bind(REPO_ID_KEY)
            .bind(repo_id)
            .execute(sql)
            .await?;
        Ok(())
    }

    pub async fn get_or_init_repo_id(sql: &SqlitePool) -> Res<String> {
        if let Some(repo_id) = get_repo_id(sql).await? {
            return Ok(repo_id);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let repo_id = format!("repo-{}-{:016x}", now, rand::random::<u64>());
        set_repo_id(sql, &repo_id).await?;
        Ok(repo_id)
    }

    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default, PartialEq, Eq)]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
    pub struct RepoConfig {
        pub known_repos: Vec<KnownRepoEntry>,
        pub last_used_repo_id: Option<String>,
    }
    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
    pub struct KnownRepoEntry {
        pub id: String,
        pub path: String,
        pub created_at_unix_secs: i64,
        pub last_opened_at_unix_secs: i64,
    }
    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Default)]
    pub struct SyncConfig {
        pub known_devices: Vec<SyncDeviceEntry>,
    }

    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
    pub struct SyncDeviceEntry {
        pub endpoint_id: String,
        #[serde(default = "default_sync_device_name")]
        pub name: String,
        pub added_at_unix_secs: i64,
        pub last_connected_at_unix_secs: Option<i64>,
    }

    fn default_sync_device_name() -> String {
        "unknown-device".to_string()
    }

    const REPO_CONFIG_KEY: &str = "repo_config";
    const SYNC_CONFIG_KEY: &str = "sync_config";

    pub async fn get_repo_config(sql: &SqlitePool) -> Res<RepoConfig> {
        let rec = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
            .bind(REPO_CONFIG_KEY)
            .fetch_optional(sql)
            .await?;
        let state = match rec {
            Some(json) => serde_json::from_str::<RepoConfig>(&json)?,
            None => RepoConfig::default(),
        };
        Ok(state)
    }

    pub async fn set_repo_config(sql: &SqlitePool, state: &RepoConfig) -> Res<()> {
        let json = serde_json::to_string(state)?;
        sqlx::query("INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
        .bind(REPO_CONFIG_KEY)
        .bind(&json)
        .execute(sql)
        .await?;
        Ok(())
    }

    pub async fn get_sync_config(sql: &SqlitePool) -> Res<SyncConfig> {
        let rec = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
            .bind(SYNC_CONFIG_KEY)
            .fetch_optional(sql)
            .await?;
        let state = match rec {
            Some(json) => serde_json::from_str::<SyncConfig>(&json)?,
            None => SyncConfig::default(),
        };
        Ok(state)
    }

    pub async fn set_sync_config(sql: &SqlitePool, state: &SyncConfig) -> Res<()> {
        let json = serde_json::to_string(state)?;
        sqlx::query("INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
            .bind(SYNC_CONFIG_KEY)
            .bind(&json)
            .execute(sql)
            .await?;
        Ok(())
    }
}
