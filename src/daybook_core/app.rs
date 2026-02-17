use crate::interlude::*;
use automerge::transaction::Transactable;
use fs4::fs_std::FileExt;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

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

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum InitState {
    None,
    Created {
        doc_id_app: DocumentId,
        doc_id_drawer: DocumentId,
    },
}

pub const INIT_STATE_KEY: &str = "init_state";
pub const LOCAL_USER_PATH_KEY: &str = "local_user_path";
pub const REPO_CONFIG_KEY: &str = "repo_config";
pub const REPO_MARKER_FILE: &str = "db.repo.txt";

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoConfig {
    pub known_repos: Vec<KnownRepoEntry>,
    pub last_used_repo_id: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct KnownRepoEntry {
    pub id: String,
    pub path: String,
    pub created_at_unix_secs: i64,
    pub last_opened_at_unix_secs: i64,
}

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

pub fn app_data_dir() -> Res<std::path::PathBuf> {
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
            created_at_unix_secs: unix_now_secs()?,
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
    pub acx_stop: tokio::sync::Mutex<Option<utils_rs::am::AmCtxStopToken>>,
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

        let am_config = utils_rs::am::Config {
            storage: utils_rs::am::StorageConfig::Disk {
                path: layout.samod_root.clone(),
            },
            peer_id: options.peer_id,
        };
        let sql_config = SqlConfig {
            database_url: format!("sqlite://{}", layout.sqlite_path.display()),
        };
        let sql = SqlCtx::new(&sql_config.database_url).await?;

        let local_user_path = match get_local_user_path(&sql.db_pool).await? {
            Some(path) => path,
            None => {
                let default_path = "/default-device".to_string();
                set_local_user_path(&sql.db_pool, &default_path).await?;
                default_path
            }
        };
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(
            &daybook_types::doc::UserPath::from(local_user_path.clone()),
        );

        let (acx, acx_stop) =
            utils_rs::am::AmCtx::boot(am_config, Option::<samod::AlwaysAnnounce>::None).await?;
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
            run_repo_init_dance(
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
}

fn unix_now_secs() -> Res<i64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .wrap_err("system clock before unix epoch")?
        .as_secs() as i64)
}

pub fn repo_layout(repo_root: &std::path::Path) -> Res<RepoLayout> {
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

pub async fn list_known_repos(sql: &SqlitePool) -> Res<Vec<KnownRepoEntry>> {
    let mut known_repos = get_repo_config(sql).await?.known_repos;
    known_repos.sort_by(|left, right| {
        right
            .last_opened_at_unix_secs
            .cmp(&left.last_opened_at_unix_secs)
            .then_with(|| left.path.cmp(&right.path))
    });
    Ok(known_repos)
}

pub async fn get_last_used_repo(sql: &SqlitePool) -> Res<Option<KnownRepoEntry>> {
    let repo_config = get_repo_config(sql).await?;
    let Some(last_used_repo_id) = repo_config.last_used_repo_id else {
        return Ok(None);
    };
    Ok(repo_config
        .known_repos
        .into_iter()
        .find(|repo| repo.id == last_used_repo_id))
}

pub async fn upsert_known_repo(
    sql: &SqlitePool,
    repo_root: &std::path::Path,
) -> Res<KnownRepoEntry> {
    let repo_root = std::path::absolute(repo_root)
        .wrap_err_with(|| format!("error absolutizing repo path {}", repo_root.display()))?;
    let repo_path = repo_root.display().to_string();
    let now_unix_secs = unix_now_secs()?;
    let mut repo_config = get_repo_config(sql).await?;
    if let Some(existing_repo) = repo_config
        .known_repos
        .iter_mut()
        .find(|repo| repo.path == repo_path)
    {
        existing_repo.last_opened_at_unix_secs = now_unix_secs;
        let repo = existing_repo.clone();
        repo_config.last_used_repo_id = Some(repo.id.clone());
        set_repo_config(sql, &repo_config).await?;
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
    set_repo_config(sql, &repo_config).await?;
    Ok(repo)
}

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

pub mod version_updates {
    use crate::interlude::*;

    use automerge::{transaction::Transactable, ActorId, AutoCommit, ROOT};
    use autosurgeon::reconcile_prop;

    use crate::config::ConfigStore;
    use crate::plugs::PlugsStore;
    use crate::rt::dispatch::DispatchStore;
    use crate::rt::triage::DocTriageWorkerStateStore;
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
            DocTriageWorkerStateStore::prop().as_ref(),
            DocTriageWorkerStateStore::default(),
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
    let init_state = get_init_state(sql).await?;
    let (handle_app, handle_drawer) = if let InitState::Created {
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
        set_init_state(
            sql,
            &InitState::Created {
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

pub async fn run_repo_init_dance(
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

    let (drawer_repo, drawer_stop) = DrawerRepo::load(
        acx.clone(),
        doc_drawer.document_id().clone(),
        local_actor_id.clone(),
        Arc::new(std::sync::Mutex::new(
            crate::drawer::lru::KeyedLruPool::new(1000),
        )),
        Arc::new(std::sync::Mutex::new(
            crate::drawer::lru::KeyedLruPool::new(1000),
        )),
    )
    .await?;
    let blobs_repo = BlobsRepo::new(blobs_root).await?;
    let (plugs_repo, plugs_stop) = PlugsRepo::load(
        acx.clone(),
        Arc::clone(&blobs_repo),
        doc_app.document_id().clone(),
        local_actor_id.clone(),
    )
    .await
    .wrap_err("error loading plugs repo during init dance")?;
    drawer_repo.set_plugs_repo(Arc::clone(&plugs_repo));

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

    plugs_repo.ensure_system_plugs().await?;

    drawer_stop.stop().await?;
    plugs_stop.stop().await?;
    config_stop.stop().await?;
    tables_stop.stop().await?;
    dispatch_stop.stop().await?;
    Ok(())
}
