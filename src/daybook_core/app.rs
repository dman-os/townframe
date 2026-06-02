use crate::interlude::*;

use crate::repo::RepoCtx;

use sqlx::SqlitePool;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(90);
pub use sqlx_utils_rs::SqlCtx;

#[derive(Debug, Clone)]
pub struct SqlConfig {
    database_path: Option<PathBuf>,
}

impl SqlConfig {
    pub fn file(database_path: impl Into<PathBuf>) -> Self {
        Self {
            database_path: Some(database_path.into()),
        }
    }

    pub fn memory() -> Self {
        Self {
            database_path: None,
        }
    }
}

pub async fn open_sql_ctx(config: SqlConfig) -> Res<SqlCtx> {
    let db_pool = match config.database_path {
        Some(database_path) => {
            if let Some(parent) = database_path.parent() {
                std::fs::create_dir_all(parent).wrap_err_with(|| {
                    format!("Failed to create database directory: {}", parent.display())
                })?;
            }

            let connect_options = sqlx_utils_rs::sqlite_file_connect_options_with_wal_busy(
                &database_path,
                SQLITE_BUSY_TIMEOUT,
            )?;
            sqlx_utils_rs::open_sqlite_pool(&database_path, connect_options, 1).await?
        }
        None => {
            let connect_options = sqlx::sqlite::SqliteConnectOptions::from_str("sqlite::memory:")?;
            SqlitePool::connect_with(connect_options).await?
        }
    };

    sqlx::query(
        r#"
            CREATE TABLE IF NOT EXISTS kvstore (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) STRICT
            "#,
    )
    .execute(&db_pool)
    .await?;

    Ok(SqlCtx::from_single_pool(db_pool))
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub app_data_dir: std::path::PathBuf,
    pub sql: SqlConfig,
    pub default_repo_root: std::path::PathBuf,
}

impl AppConfig {
    pub fn load() -> Res<Self> {
        let app_data_dir = app_data_dir()?;
        let sql = SqlConfig::file(app_data_dir.join("globals.sqlite.db"));
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

pub struct AppCtx {
    pub config: AppConfig,
    pub sql: SqlCtx,
}

impl AppCtx {
    pub async fn new(config: AppConfig) -> Res<Self> {
        let sql = open_sql_ctx(config.sql.clone()).await?;
        Ok(Self { config, sql })
    }

    pub async fn load() -> Res<Self> {
        Self::new(AppConfig::load()?).await
    }
    pub async fn init_repo(
        &self,
        repo_root: &std::path::Path,
        options: crate::repo::RepoOpenOptions,
        repo_name: String,
        local_device_name: String,
    ) -> Res<Arc<RepoCtx>> {
        let rcx = RepoCtx::init(repo_root, options, repo_name, local_device_name).await?;
        if let Err(err) = crate::app::globals::upsert_known_repo(&self.sql.write_pool, &rcx).await {
            let _ = rcx.shutdown().await;
            return Err(err);
        }
        Ok(rcx)
    }

    pub async fn open_repo(
        &self,
        repo_root: &std::path::Path,
        options: crate::repo::RepoOpenOptions,
        local_device_name: String,
    ) -> Res<Arc<RepoCtx>> {
        let rcx = RepoCtx::open(repo_root, options, local_device_name).await?;
        if let Err(err) = crate::app::globals::upsert_known_repo(&self.sql.write_pool, &rcx).await {
            let _ = rcx.shutdown().await;
            return Err(err);
        }
        Ok(rcx)
    }
}

pub mod version_updates {
    use crate::interlude::*;

    use automerge::{transaction::Transactable, ActorId, AutoCommit, ROOT};
    use autosurgeon::reconcile_prop;

    use crate::config::ConfigStore;
    use crate::plugs::PlugsStore;
    use crate::rt::init::InitStore;
    use crate::tables::TablesStore;

    pub fn version_latest() -> Res<Vec<u8>> {
        use crate::stores::AmStore;
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
            InitStore::prop().as_ref(),
            InitStore::default(),
        )?;
        Ok(doc.save_nocompress())
    }
}

pub mod globals {
    use sqlx::SqlitePool;

    use crate::interlude::*;

    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
    pub struct KnownRepoEntry {
        pub id: String,
        pub checkout_id: String,
        #[serde(default)]
        pub name: String,
        pub path: String,
        pub created_at_unix_secs: i64,
        pub last_opened_at_unix_secs: i64,
    }
    pub async fn upsert_known_repo(
        sql: &SqlitePool,
        rcx: &crate::repo::RepoCtx,
    ) -> Res<KnownRepoEntry> {
        let repo_path = rcx.layout.repo_root.display().to_string();
        let now_unix_secs = jiff::Timestamp::now().as_second();
        let mut repo_config = get_repo_config(sql).await?;
        if let Some(existing_repo) = repo_config
            .known_repos
            .iter_mut()
            .find(|repo| repo.path == repo_path)
        {
            existing_repo.last_opened_at_unix_secs = now_unix_secs;
            if existing_repo.name.is_empty() {
                existing_repo.name = rcx.repo_name.clone();
            }
            let repo = existing_repo.clone();
            repo_config.last_used_repo_id = Some(repo.id.clone());
            set_repo_config(sql, &repo_config).await?;
            return Ok(repo);
        }

        let repo = KnownRepoEntry {
            id: repo_path.clone(),
            checkout_id: rcx.checkout_id.clone(),
            name: rcx.repo_name.clone(),
            path: repo_path,
            created_at_unix_secs: now_unix_secs,
            last_opened_at_unix_secs: now_unix_secs,
        };
        repo_config.last_used_repo_id = Some(repo.id.clone());
        repo_config.known_repos.push(repo.clone());
        set_repo_config(sql, &repo_config).await?;
        Ok(repo)
    }

    const REPO_CONFIG_KEY: &str = "repo_config";
    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default, PartialEq, Eq)]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
    pub struct RepoConfig {
        pub known_repos: Vec<KnownRepoEntry>,
        pub last_used_repo_id: Option<String>,
    }
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
}
