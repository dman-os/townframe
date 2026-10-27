use crate::interlude::*;

use crate::repo::RepoCtx;
use crate::sqlite::queries;

pub use sqlite_rs::Database;
use std::path::PathBuf;

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

pub async fn open_database(runtime: &sqlite_rs::SqliteRuntime, config: SqlConfig) -> Res<Database> {
    let sql = match config.database_path {
        Some(database_path) => {
            if let Some(parent) = database_path.parent() {
                tokio::fs::create_dir_all(parent).await.wrap_err_with(|| {
                    format!("Failed to create database directory: {}", parent.display())
                })?;
            }
            runtime.database(database_path, queries::MIGRATIONS, queries::MIGRATION_TABLE)
        }
        None => runtime.ephemeral(queries::MIGRATIONS)?,
    };
    sql.migrate().await?;
    Ok(sql)
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
    pub sql: Database,
    pub sqlite_runtime: Arc<sqlite_rs::SqliteRuntime>,
}

impl AppCtx {
    pub async fn new(config: AppConfig) -> Res<Self> {
        let sqlite_runtime = Arc::new(sqlite_rs::SqliteRuntime::new(default()));
        let sql = open_database(&sqlite_runtime, config.sql.clone()).await?;
        Ok(Self {
            config,
            sql,
            sqlite_runtime,
        })
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
        let rcx = RepoCtx::init(
            Arc::clone(&self.sqlite_runtime),
            repo_root,
            options,
            repo_name,
            local_device_name,
        )
        .await?;
        if let Err(err) = crate::app::globals::upsert_known_repo(&self.sql, &rcx).await {
            rcx.shutdown()
                .await
                .inspect_err(|err| error!("error shutting down repo: {err}"))
                .ok();
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
        let rcx = RepoCtx::open(
            Arc::clone(&self.sqlite_runtime),
            repo_root,
            options,
            local_device_name,
        )
        .await?;
        if let Err(err) = crate::app::globals::upsert_known_repo(&self.sql, &rcx).await {
            rcx.shutdown()
                .await
                .inspect_err(|err| error!("error shutting down repo: {err}"))
                .ok();
            return Err(err);
        }
        Ok(rcx)
    }
}

pub mod version_updates {
    use crate::interlude::*;

    use automerge::ROOT;
    use automerge::transaction::Transactable;
    use autosurgeon::reconcile_prop;

    use crate::config::ConfigStore;
    use crate::rt::init::InitStore;
    use crate::tables::TablesStore;

    pub fn version_latest() -> Res<Vec<u8>> {
        use crate::stores::AmStore;
        let mut doc = automerge::Automerge::new();
        doc.transact(|tx| {
            tx.put(ROOT, "version", "0")?;
            tx.put(ROOT, "$schema", "daybook.app")?;
            reconcile_prop(
                tx,
                ROOT,
                TablesStore::prop().as_ref(),
                TablesStore::default(),
            )
            .map_err(|_| automerge::AutomergeError::Fail)?;
            reconcile_prop(
                tx,
                ROOT,
                ConfigStore::prop().as_ref(),
                ConfigStore::default(),
            )
            .map_err(|_| automerge::AutomergeError::Fail)?;
            reconcile_prop(tx, ROOT, InitStore::prop().as_ref(), InitStore::default())
                .map_err(|_| automerge::AutomergeError::Fail)?;
            Ok::<_, automerge::AutomergeError>(())
        })
        .map_err(|err| ferr!("{err:?}"))?;
        Ok(doc.save_nocompress())
    }
}

pub mod globals {
    use crate::interlude::*;
    use crate::sqlite::queries;

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
        sql: &Database,
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
    pub async fn get_repo_config(sql: &Database) -> Res<RepoConfig> {
        let rec = queries::kv_get(sql, REPO_CONFIG_KEY.to_owned()).await?;
        let state = match rec {
            Some(json) => serde_json::from_str::<RepoConfig>(&json)?,
            None => RepoConfig::default(),
        };
        Ok(state)
    }

    pub async fn set_repo_config(sql: &Database, state: &RepoConfig) -> Res<()> {
        let json = serde_json::to_string(state)?;
        queries::kv_upsert(sql, REPO_CONFIG_KEY.to_owned(), json).await?;
        Ok(())
    }
}
