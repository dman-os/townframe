use crate::app::globals::KnownRepoEntry;
use crate::interlude::*;

use crate::app::*;

use fs4::fs_std::FileExt;
use sqlx::SqlitePool;

const REPO_MARKER_FILE: &str = "db.repo.txt";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoLayout {
    pub repo_root: std::path::PathBuf,
    pub samod_root: std::path::PathBuf,
    pub sqlite_path: std::path::PathBuf,
    pub blobs_root: std::path::PathBuf,
    pub marker_path: std::path::PathBuf,
    pub lock_path: std::path::PathBuf,
}

#[derive(Debug, Clone)]
pub struct RepoOpenOptions {
    pub ensure_initialized: bool,
    pub peer_id: String,
    pub ws_connector_url: Option<String>,
}

impl Default for RepoOpenOptions {
    fn default() -> Self {
        Self {
            ensure_initialized: false,
            peer_id: "daybook_client".to_string(),
            ws_connector_url: None,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct RepoLockInfo {
    pid: u32,
    created_at_unix_secs: i64,
}

pub struct RepoLockGuard {
    file: std::fs::File,
    lock_path: std::path::PathBuf,
}

impl RepoLockGuard {
    pub fn acquire(lock_path: &std::path::Path) -> Res<Self> {
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(lock_path)
            .wrap_err_with(|| format!("error opening repo lock file {}", lock_path.display()))?;

        file.try_lock_exclusive().map_err(|err| {
            let holder = std::fs::read_to_string(lock_path)
                .ok()
                .and_then(|content| serde_json::from_str::<RepoLockInfo>(&content).ok());
            if let Some(holder) = holder {
                eyre::eyre!(
                    "repo is already in use by pid={} (lock file: {})",
                    holder.pid,
                    lock_path.display()
                )
            } else {
                eyre::eyre!(
                    "repo is already in use (lock file: {}, cause: {})",
                    lock_path.display(),
                    err
                )
            }
        })?;

        let lock_info = RepoLockInfo {
            pid: std::process::id(),
            created_at_unix_secs: jiff::Timestamp::now().as_second(),
        };
        file.set_len(0)?;
        let json = serde_json::to_string(&lock_info)?;
        std::io::Write::write_all(&mut file, json.as_bytes())?;
        std::io::Write::flush(&mut file)?;
        Ok(Self {
            file,
            lock_path: lock_path.to_path_buf(),
        })
    }
}

impl Drop for RepoLockGuard {
    fn drop(&mut self) {
        if let Err(err) = self.file.unlock() {
            error!(?err, path = ?self.lock_path, "error unlocking repo lock");
        }
    }
}

pub struct RepoCtx {
    pub layout: RepoLayout,
    pub lock_guard: RepoLockGuard,
    pub sql: SqlCtx,
    pub acx: AmCtx,
    pub acx_stop: tokio::sync::Mutex<Option<am_utils_rs::AmCtxStopToken>>,
    pub doc_app: samod::DocHandle,
    pub doc_drawer: samod::DocHandle,
    pub local_actor_id: automerge::ActorId,
    pub local_user_path: String,
}

impl RepoCtx {
    pub async fn open(
        global_ctx: &GlobalCtx,
        repo_root: &std::path::Path,
        options: RepoOpenOptions,
    ) -> Res<Self> {
        let layout = repo_layout(repo_root)?;
        let lock_guard = RepoLockGuard::acquire(&layout.lock_path)?;

        let is_initialized = is_repo_initialized(&layout.repo_root).await?;
        if !options.ensure_initialized && !is_initialized {
            eyre::bail!(
                "repo not initialized at path {} (missing marker {})",
                layout.repo_root.display(),
                layout.marker_path.display()
            );
        }

        let am_config = am_utils_rs::Config {
            storage: am_utils_rs::StorageConfig::Disk {
                path: layout.samod_root.clone(),
            },
            peer_id: options.peer_id,
        };
        let sql_config = SqlConfig {
            database_url: format!("sqlite://{}", layout.sqlite_path.display()),
        };
        let sql = SqlCtx::new(&sql_config.database_url).await?;

        let local_user_path = match globals::get_local_user_path(&sql.db_pool).await? {
            Some(path) => path,
            None => {
                let default_path = "/default-device".to_string();
                globals::set_local_user_path(&sql.db_pool, &default_path).await?;
                default_path
            }
        };
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(
            &daybook_types::doc::UserPath::from(local_user_path.clone()),
        );

        let (acx, acx_stop) =
            am_utils_rs::AmCtx::boot(am_config, Option::<samod::AlwaysAnnounce>::None).await?;
        if let Some(ws_connector_url) = options.ws_connector_url {
            acx.spawn_ws_connector(ws_connector_url.into());
        }

        let doc_app_cell = tokio::sync::OnceCell::new();
        let doc_drawer_cell = tokio::sync::OnceCell::new();
        init_from_globals(&acx, &sql.db_pool, &doc_app_cell, &doc_drawer_cell).await?;
        let doc_app = doc_app_cell
            .get()
            .expect("doc_app cell should be initialized")
            .clone();
        let doc_drawer = doc_drawer_cell
            .get()
            .expect("doc_drawer cell should be initialized")
            .clone();

        if options.ensure_initialized && !is_initialized {
            Self::run_repo_init_dance(
                &acx,
                &doc_app,
                &doc_drawer,
                &local_actor_id,
                &local_user_path,
                layout.blobs_root.clone(),
            )
            .await?;
            mark_repo_initialized(&layout.repo_root).await?;
        }

        let _repo_entry = upsert_known_repo(&global_ctx.sql.db_pool, &layout.repo_root).await?;
        Ok(Self {
            layout,
            lock_guard,
            sql,
            acx,
            acx_stop: Some(acx_stop).into(),
            doc_app,
            doc_drawer,
            local_actor_id,
            local_user_path,
        })
    }

    async fn run_repo_init_dance(
        acx: &AmCtx,
        doc_app: &samod::DocHandle,
        doc_drawer: &samod::DocHandle,
        local_actor_id: &automerge::ActorId,
        local_user_path: &str,
        blobs_root: std::path::PathBuf,
    ) -> Res<()> {
        use crate::blobs::BlobsRepo;
        use crate::config::ConfigRepo;
        use crate::drawer::DrawerRepo;
        use crate::plugs::PlugsRepo;
        use crate::rt::dispatch::DispatchRepo;
        use crate::tables::TablesRepo;

        let blobs_repo = BlobsRepo::new(blobs_root).await?;
        let (plugs_repo, plugs_stop) = PlugsRepo::load(
            acx.clone(),
            Arc::clone(&blobs_repo),
            doc_app.document_id().clone(),
            local_actor_id.clone(),
        )
        .await
        .wrap_err("error loading plugs repo during init dance")?;

        let (_config_repo, config_stop) = ConfigRepo::load(
            acx.clone(),
            doc_app.document_id().clone(),
            Arc::clone(&plugs_repo),
            daybook_types::doc::UserPath::from(local_user_path.to_string()),
        )
        .await?;
        let (_tables_repo, tables_stop) = TablesRepo::load(
            acx.clone(),
            doc_app.document_id().clone(),
            local_actor_id.clone(),
        )
        .await?;
        let (_dispatch_repo, dispatch_stop) = DispatchRepo::load(
            acx.clone(),
            doc_app.document_id().clone(),
            local_actor_id.clone(),
        )
        .await?;
        let (_drawer_repo, drawer_stop) = DrawerRepo::load(
            acx.clone(),
            doc_drawer.document_id().clone(),
            local_actor_id.clone(),
            Arc::new(std::sync::Mutex::new(
                crate::drawer::lru::KeyedLruPool::new(1000),
            )),
            Arc::new(std::sync::Mutex::new(
                crate::drawer::lru::KeyedLruPool::new(1000),
            )),
            #[cfg(not(test))]
            Arc::clone(&plugs_repo),
            #[cfg(test)]
            Some(Arc::clone(&plugs_repo)),
        )
        .await?;

        plugs_repo.ensure_system_plugs().await?;

        drawer_stop.stop().await?;
        plugs_stop.stop().await?;
        config_stop.stop().await?;
        tables_stop.stop().await?;
        dispatch_stop.stop().await?;
        Ok(())
    }
}

fn repo_layout(repo_root: &std::path::Path) -> Res<RepoLayout> {
    let repo_root = std::path::absolute(repo_root)
        .wrap_err_with(|| format!("error absolutizing repo root {}", repo_root.display()))?;
    Ok(RepoLayout {
        repo_root: repo_root.clone(),
        samod_root: repo_root.join("samod"),
        sqlite_path: repo_root.join("sqlite.db"),
        blobs_root: repo_root.join("blobs"),
        marker_path: repo_root.join(REPO_MARKER_FILE),
        lock_path: repo_root.join("repo.lock"),
    })
}

pub async fn mark_repo_initialized(repo_root: &std::path::Path) -> Res<()> {
    let layout = repo_layout(repo_root)?;
    if let Some(parent) = layout.marker_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let _file = tokio::fs::File::create(layout.marker_path).await?;
    Ok(())
}

pub async fn is_repo_initialized(repo_root: &std::path::Path) -> Res<bool> {
    let layout = repo_layout(repo_root)?;
    Ok(utils_rs::file_exists(&layout.marker_path).await?)
}

pub async fn upsert_known_repo(
    sql: &SqlitePool,
    repo_root: &std::path::Path,
) -> Res<KnownRepoEntry> {
    let repo_root = std::path::absolute(repo_root)
        .wrap_err_with(|| format!("error absolutizing repo path {}", repo_root.display()))?;
    let repo_path = repo_root.display().to_string();
    let now_unix_secs = jiff::Timestamp::now().as_second();
    let mut repo_config = globals::get_repo_config(sql).await?;
    if let Some(existing_repo) = repo_config
        .known_repos
        .iter_mut()
        .find(|repo| repo.path == repo_path)
    {
        existing_repo.last_opened_at_unix_secs = now_unix_secs;
        let repo = existing_repo.clone();
        repo_config.last_used_repo_id = Some(repo.id.clone());
        globals::set_repo_config(sql, &repo_config).await?;
        return Ok(repo);
    }

    let repo = KnownRepoEntry {
        id: repo_path.clone(),
        path: repo_path,
        created_at_unix_secs: now_unix_secs,
        last_opened_at_unix_secs: now_unix_secs,
    };
    repo_config.last_used_repo_id = Some(repo.id.clone());
    repo_config.known_repos.push(repo.clone());
    globals::set_repo_config(sql, &repo_config).await?;
    Ok(repo)
}
