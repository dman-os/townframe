use crate::app::globals::KnownRepoEntry;
use crate::interlude::*;

use crate::app::*;

use am_utils_rs::partition::PartitionStore;
use fs4::fs_std::FileExt;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::SqlitePool;
use std::str::FromStr;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

const REPO_MARKER_FILE: &str = "db.repo.txt";
const REPO_USER_ID_KEY: &str = "repo.user_id";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoLayout {
    pub repo_root: std::path::PathBuf,
    pub samod_root: std::path::PathBuf,
    pub sqlite_path: std::path::PathBuf,
    pub blobs_root: std::path::PathBuf,
    pub marker_path: std::path::PathBuf,
    pub lock_path: std::path::PathBuf,
}

#[derive(Debug, Clone, Default)]
pub struct RepoOpenOptions {}

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
    pub partition_store: Arc<PartitionStore>,

    pub big_repo: SharedBigRepo,
    big_repo_stop: std::sync::Mutex<Option<am_utils_rs::BigRepoStopToken>>,

    pub doc_app: samod::DocHandle,
    pub doc_drawer: samod::DocHandle,

    pub local_actor_id: automerge::ActorId,
    pub local_user_path: String,
    pub local_device_name: String,
    pub repo_id: String,

    pub iroh_public_key: String,
    pub iroh_secret_key: iroh::SecretKey,
}

impl RepoCtx {
    pub async fn shutdown(&self) -> Res<()> {
        let stop = self
            .big_repo_stop
            .lock()
            .expect(ERROR_MUTEX)
            .take()
            .ok_or_eyre("big repo stop token missing")?;
        stop.stop().await?;
        Ok(())
    }

    pub async fn open(
        repo_root: &std::path::Path,
        options: RepoOpenOptions,
        local_device_name: String,
    ) -> Res<Self> {
        let layout = repo_layout(repo_root)?;
        let lock_guard = RepoLockGuard::acquire(&layout.lock_path)?;
        if !is_repo_initialized(&layout.repo_root).await? {
            eyre::bail!(
                "repo not initialized at path {} (missing marker {})",
                layout.repo_root.display(),
                layout.marker_path.display()
            );
        }
        Self::open_inner(layout, lock_guard, options, local_device_name, false).await
    }

    pub async fn init(
        repo_root: &std::path::Path,
        options: RepoOpenOptions,
        local_device_name: String,
    ) -> Res<Self> {
        let layout = repo_layout(repo_root)?;
        let lock_guard = RepoLockGuard::acquire(&layout.lock_path)?;
        if is_repo_initialized(&layout.repo_root).await? {
            eyre::bail!(
                "repo already initialized at path {}",
                layout.repo_root.display()
            );
        }
        Self::open_inner(layout, lock_guard, options, local_device_name, true).await
    }

    async fn open_inner(
        layout: RepoLayout,
        lock_guard: RepoLockGuard,
        _options: RepoOpenOptions,
        local_device_name: String,
        initialize_repo: bool,
    ) -> Res<Self> {
        cleanup_blobs_staging_dir(&layout.blobs_root).await?;
        let sql_config = SqlConfig {
            database_url: format!("sqlite://{}", layout.sqlite_path.display()),
        };
        let sql = SqlCtx::new(&sql_config.database_url).await?;
        let repo_id = if initialize_repo {
            crate::app::globals::get_or_init_repo_id(&sql.db_pool).await?
        } else {
            crate::app::globals::get_repo_id(&sql.db_pool)
                .await?
                .ok_or_eyre("repo_id missing in initialized repo")?
        };
        let identity =
            crate::secrets::SecretRepo::load_or_init_identity(&sql.db_pool, &repo_id).await?;
        let iroh_public_key = identity.iroh_public_key.to_string();
        let repo_user_id = if initialize_repo {
            get_or_init_repo_user_id(&sql.db_pool).await?
        } else {
            get_repo_user_id(&sql.db_pool)
                .await?
                .ok_or_eyre("repo_user_id missing in initialized repo; migration required")?
        };
        let device_bs58 =
            utils_rs::hash::encode_base58_multibase(identity.iroh_public_key.as_bytes());
        let device_id = format!(
            "{}{}",
            daybook_types::doc::user_path::DEVICE_ID_PREFIX,
            device_bs58
        );
        let local_user_path = format!("/{repo_user_id}/{device_id}");
        let peer_id = format!("/{}/{}", identity.repo_id, iroh_public_key);
        let am_config = am_utils_rs::repo::Config {
            storage: am_utils_rs::repo::StorageConfig::Disk {
                path: layout.samod_root.clone(),
                big_repo_sqlite_url: None,
            },
            peer_id,
        };
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(
            &daybook_types::doc::UserPath::from(local_user_path.clone()),
        );

        let big_repo_sqlite_url = format!(
            "sqlite://{}",
            layout.repo_root.join("big_repo.sqlite").display()
        );
        let connect_options = SqliteConnectOptions::from_str(&big_repo_sqlite_url)
            .wrap_err_with(|| format!("invalid sqlite url: {big_repo_sqlite_url}"))?
            .create_if_missing(true);
        let partition_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .wrap_err("failed connecting big repo sqlite")?;
        let (partition_events_tx, _) =
            broadcast::channel(am_utils_rs::sync::protocol::DEFAULT_SUBSCRIPTION_CAPACITY);
        let partition_forwarder_cancel = CancellationToken::new();
        let partition_forwarders = Arc::new(utils_rs::AbortableJoinSet::new());
        let partition_store = Arc::new(PartitionStore::new(
            partition_pool,
            partition_events_tx,
            partition_forwarder_cancel.clone(),
            Arc::clone(&partition_forwarders),
        ));
        partition_store.ensure_schema().await?;
        let (big_repo, big_repo_stop) = am_utils_rs::BigRepo::boot_with_partition_store(
            am_config,
            Arc::clone(&partition_store),
            partition_forwarder_cancel,
            partition_forwarders,
        )
        .await?;

        let doc_app_cell = tokio::sync::OnceCell::new();
        let doc_drawer_cell = tokio::sync::OnceCell::new();
        init_from_globals(
            &big_repo,
            &sql.db_pool,
            &doc_app_cell,
            &doc_drawer_cell,
            if initialize_repo {
                crate::app::InitFromGlobalsMode::CreateFresh
            } else {
                crate::app::InitFromGlobalsMode::RequireExisting
            },
        )
        .await?;
        let doc_app = doc_app_cell
            .get()
            .expect("doc_app cell should be initialized")
            .clone();
        let doc_drawer = doc_drawer_cell
            .get()
            .expect("doc_drawer cell should be initialized")
            .clone();
        ensure_expected_partitions_for_docs(
            &big_repo,
            doc_app.document_id(),
            doc_drawer.document_id(),
        )
        .await?;

        if initialize_repo {
            Self::run_repo_init_dance(
                &big_repo,
                &doc_app,
                &doc_drawer,
                &local_user_path,
                &sql.db_pool,
                layout.blobs_root.clone(),
            )
            .await?;
            mark_repo_initialized(&layout.repo_root).await?;
        }

        Ok(Self {
            layout,
            lock_guard,
            sql,
            partition_store,
            big_repo,
            big_repo_stop: std::sync::Mutex::new(Some(big_repo_stop)),
            doc_app,
            doc_drawer,
            local_actor_id,
            local_user_path,
            repo_id: identity.repo_id,
            iroh_public_key,
            iroh_secret_key: identity.iroh_secret_key,
            local_device_name,
        })
    }

    async fn run_repo_init_dance(
        big_repo: &SharedBigRepo,
        doc_app: &samod::DocHandle,
        doc_drawer: &samod::DocHandle,
        local_user_path: &str,
        sql: &SqlitePool,
        blobs_root: std::path::PathBuf,
    ) -> Res<()> {
        use crate::blobs::BlobsRepo;
        use crate::config::ConfigRepo;
        use crate::drawer::DrawerRepo;
        use crate::plugs::PlugsRepo;
        use crate::rt::dispatch::DispatchRepo;
        use crate::tables::TablesRepo;

        let blobs_repo = BlobsRepo::new(
            blobs_root.clone(),
            local_user_path.to_string(),
            Arc::new(crate::blobs::PartitionStoreMembershipWriter::new(
                big_repo.partition_store(),
            )),
        )
        .await?;
        let mut plugs_repo: Option<Arc<PlugsRepo>> = None;
        let mut plugs_stop: Option<crate::repos::RepoStopToken> = None;
        let mut config_stop: Option<crate::repos::RepoStopToken> = None;
        let mut tables_stop: Option<crate::repos::RepoStopToken> = None;
        let mut dispatch_stop: Option<crate::repos::RepoStopToken> = None;
        let mut drawer_stop: Option<crate::repos::RepoStopToken> = None;

        let init_result: Res<()> = async {
            let (repo, stop) = PlugsRepo::load(
                Arc::clone(big_repo),
                Arc::clone(&blobs_repo),
                doc_app.document_id().clone(),
                daybook_types::doc::UserPath::from(local_user_path.to_string()),
            )
            .await
            .wrap_err("error loading plugs repo during init dance")?;
            plugs_repo = Some(repo);
            plugs_stop = Some(stop);

            let (config_repo, stop) = ConfigRepo::load(
                Arc::clone(big_repo),
                doc_app.document_id().clone(),
                Arc::clone(plugs_repo.as_ref().expect("plugs repo must be loaded")),
                daybook_types::doc::UserPath::from(local_user_path.to_string()),
                sql.clone(),
            )
            .await?;
            config_stop = Some(stop);
            let config_user_path = daybook_types::doc::user_path::for_repo(
                &daybook_types::doc::UserPath::from(local_user_path.to_string()),
                "config-repo",
            )?;
            let config_actor_id = daybook_types::doc::user_path::to_actor_id(&config_user_path);
            config_repo
                .upsert_actor_user_path(config_actor_id, config_user_path)
                .await?;
            let plugs_user_path = daybook_types::doc::user_path::for_repo(
                &daybook_types::doc::UserPath::from(local_user_path.to_string()),
                "plugs-repo",
            )?;
            let plugs_actor_id = daybook_types::doc::user_path::to_actor_id(&plugs_user_path);
            config_repo
                .upsert_actor_user_path(plugs_actor_id, plugs_user_path)
                .await?;

            let (_tables_repo, stop) = TablesRepo::load(
                Arc::clone(big_repo),
                doc_app.document_id().clone(),
                daybook_types::doc::UserPath::from(local_user_path.to_string()),
            )
            .await?;
            tables_stop = Some(stop);
            let tables_user_path = daybook_types::doc::user_path::for_repo(
                &daybook_types::doc::UserPath::from(local_user_path.to_string()),
                "tables-repo",
            )?;
            let tables_actor_id = daybook_types::doc::user_path::to_actor_id(&tables_user_path);
            config_repo
                .upsert_actor_user_path(tables_actor_id, tables_user_path)
                .await?;

            let (_dispatch_repo, stop) = DispatchRepo::load(
                Arc::clone(big_repo),
                doc_app.document_id().clone(),
                daybook_types::doc::UserPath::from(local_user_path.to_string()),
                sql.clone(),
            )
            .await?;
            dispatch_stop = Some(stop);
            let dispatch_user_path = daybook_types::doc::user_path::for_repo(
                &daybook_types::doc::UserPath::from(local_user_path.to_string()),
                "dispatch-repo",
            )?;
            let dispatch_actor_id = daybook_types::doc::user_path::to_actor_id(&dispatch_user_path);
            config_repo
                .upsert_actor_user_path(dispatch_actor_id, dispatch_user_path)
                .await?;

            let (_drawer_repo, stop) = DrawerRepo::load(
                Arc::clone(big_repo),
                doc_drawer.document_id().clone(),
                daybook_types::doc::UserPath::from(local_user_path.to_string()),
                blobs_root
                    .parent()
                    .ok_or_eyre("blobs root missing parent")?
                    .join("local_state"),
                Arc::new(std::sync::Mutex::new(
                    crate::drawer::lru::KeyedLruPool::new(1000),
                )),
                Arc::new(std::sync::Mutex::new(
                    crate::drawer::lru::KeyedLruPool::new(1000),
                )),
                #[cfg(not(test))]
                Arc::clone(plugs_repo.as_ref().expect("plugs repo must be loaded")),
                #[cfg(test)]
                Some(Arc::clone(
                    plugs_repo.as_ref().expect("plugs repo must be loaded"),
                )),
            )
            .await?;
            drawer_stop = Some(stop);
            let drawer_user_path = daybook_types::doc::user_path::for_repo(
                &daybook_types::doc::UserPath::from(local_user_path.to_string()),
                "drawer-repo",
            )?;
            let drawer_actor_id = daybook_types::doc::user_path::to_actor_id(&drawer_user_path);
            config_repo
                .upsert_actor_user_path(drawer_actor_id, drawer_user_path)
                .await?;

            plugs_repo
                .as_ref()
                .expect("plugs repo must be loaded")
                .ensure_system_plugs()
                .await?;

            Ok(())
        }
        .await;

        if let Err(err) = init_result {
            if let Some(stop) = drawer_stop.take() {
                let _ = stop.stop().await;
            }
            if let Some(stop) = plugs_stop.take() {
                let _ = stop.stop().await;
            }
            if let Some(stop) = config_stop.take() {
                let _ = stop.stop().await;
            }
            if let Some(stop) = tables_stop.take() {
                let _ = stop.stop().await;
            }
            if let Some(stop) = dispatch_stop.take() {
                let _ = stop.stop().await;
            }
            if let Err(shutdown_err) = blobs_repo.shutdown().await {
                return Err(err.wrap_err(format!(
                    "error shutting down blobs repo during init cleanup: {shutdown_err:?}"
                )));
            }
            return Err(err);
        }

        drawer_stop
            .expect("drawer stop token missing")
            .stop()
            .await?;
        plugs_stop.expect("plugs stop token missing").stop().await?;
        config_stop
            .expect("config stop token missing")
            .stop()
            .await?;
        tables_stop
            .expect("tables stop token missing")
            .stop()
            .await?;
        dispatch_stop
            .expect("dispatch stop token missing")
            .stop()
            .await?;
        blobs_repo.shutdown().await?;
        Ok(())
    }
}

pub(crate) async fn ensure_expected_partitions_for_docs(
    big_repo: &SharedBigRepo,
    doc_app_id: &DocumentId,
    doc_drawer_id: &DocumentId,
) -> Res<()> {
    let partition_store = big_repo.partition_store();
    for partition_id in [
        crate::drawer::DrawerRepo::replicated_partition_id_for_drawer(doc_drawer_id),
        crate::sync::CORE_DOCS_PARTITION_ID.to_string(),
        crate::blobs::BLOB_SCOPE_DOCS_PARTITION_ID.to_string(),
        crate::blobs::BLOB_SCOPE_PLUGS_PARTITION_ID.to_string(),
        crate::rt::PROCESSOR_RUNLOG_PARTITION_ID.to_string(),
    ] {
        partition_store.ensure_partition(&partition_id).await?;
    }
    partition_store
        .add_member(
            &crate::sync::CORE_DOCS_PARTITION_ID.to_string(),
            &doc_drawer_id.to_string(),
            &serde_json::json!({}),
        )
        .await?;
    partition_store
        .add_member(
            &crate::sync::CORE_DOCS_PARTITION_ID.to_string(),
            &doc_app_id.to_string(),
            &serde_json::json!({}),
        )
        .await?;
    Ok(())
}

pub async fn get_repo_user_id(sql: &SqlitePool) -> Res<Option<String>> {
    let rec = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
        .bind(REPO_USER_ID_KEY)
        .fetch_optional(sql)
        .await?;
    Ok(rec)
}

pub async fn set_repo_user_id(sql: &SqlitePool, user_id: &str) -> Res<()> {
    sqlx::query(
        "INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(REPO_USER_ID_KEY)
    .bind(user_id)
    .execute(sql)
    .await?;
    Ok(())
}

pub async fn get_or_init_repo_user_id(sql: &SqlitePool) -> Res<String> {
    if let Some(user_id) = get_repo_user_id(sql).await? {
        return Ok(user_id);
    }
    let user_id = format!(
        "{}{}",
        daybook_types::doc::user_path::USER_ID_PREFIX,
        Uuid::new_v4().bs58()
    );
    set_repo_user_id(sql, &user_id).await?;
    Ok(user_id)
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

async fn cleanup_blobs_staging_dir(blobs_root: &std::path::Path) -> Res<()> {
    let staging_root = blobs_root.join("staging");
    if tokio::fs::try_exists(&staging_root).await? {
        tokio::fs::remove_dir_all(&staging_root).await?;
    }
    tokio::fs::create_dir_all(&staging_root).await?;
    Ok(())
}

pub async fn is_repo_initialized(repo_root: &std::path::Path) -> Res<bool> {
    let layout = repo_layout(repo_root)?;
    Ok(utils_rs::file_exists(&layout.marker_path).await?)
}

pub async fn is_repo_bootstrapped(repo_root: &std::path::Path) -> Res<bool> {
    if !is_repo_initialized(repo_root).await? {
        return Ok(false);
    }
    let layout = repo_layout(repo_root)?;
    if !layout.sqlite_path.exists() {
        return Ok(false);
    }
    let sql_config = SqlConfig {
        database_url: format!("sqlite://{}", layout.sqlite_path.display()),
    };
    let sql = SqlCtx::new(&sql_config.database_url)
        .await
        .wrap_err_with(|| {
            format!(
                "failed opening repo sqlite while checking bootstrap state: {}",
                layout.sqlite_path.display()
            )
        })?;
    let init_state = crate::app::globals::get_init_state(&sql.db_pool).await?;
    Ok(matches!(
        init_state,
        crate::app::globals::InitState::Created { .. }
    ))
}

pub async fn upsert_known_repo(
    sql: &SqlitePool,
    repo_root: &std::path::Path,
) -> Res<KnownRepoEntry> {
    let repo_root = std::path::absolute(repo_root)
        .wrap_err_with(|| format!("error absolutizing repo path {}", repo_root.display()))?;
    let repo_path = repo_root.display().to_string();
    let now_unix_secs = jiff::Timestamp::now().as_second();
    let inferred_name = repo_root
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| repo_path.clone());
    let mut repo_config = globals::get_repo_config(sql).await?;
    if let Some(existing_repo) = repo_config
        .known_repos
        .iter_mut()
        .find(|repo| repo.path == repo_path)
    {
        existing_repo.last_opened_at_unix_secs = now_unix_secs;
        if existing_repo.name.is_empty() {
            existing_repo.name = inferred_name;
        }
        let repo = existing_repo.clone();
        repo_config.last_used_repo_id = Some(repo.id.clone());
        globals::set_repo_config(sql, &repo_config).await?;
        return Ok(repo);
    }

    let repo = KnownRepoEntry {
        id: repo_path.clone(),
        name: inferred_name,
        path: repo_path,
        created_at_unix_secs: now_unix_secs,
        last_opened_at_unix_secs: now_unix_secs,
    };
    repo_config.last_used_repo_id = Some(repo.id.clone());
    repo_config.known_repos.push(repo.clone());
    globals::set_repo_config(sql, &repo_config).await?;
    Ok(repo)
}

impl AppCtx {
    pub async fn init_repo(
        &self,
        repo_root: &std::path::Path,
        options: RepoOpenOptions,
        local_device_name: String,
    ) -> Res<RepoCtx> {
        let rcx = RepoCtx::init(repo_root, options, local_device_name).await?;
        if let Err(err) = upsert_known_repo(&self.sql.db_pool, repo_root).await {
            let _ = rcx.shutdown().await;
            return Err(err);
        }
        Ok(rcx)
    }

    pub async fn open_repo(
        &self,
        repo_root: &std::path::Path,
        options: RepoOpenOptions,
        local_device_name: String,
    ) -> Res<RepoCtx> {
        let rcx = RepoCtx::open(repo_root, options, local_device_name).await?;
        if let Err(err) = upsert_known_repo(&self.sql.db_pool, repo_root).await {
            let _ = rcx.shutdown().await;
            return Err(err);
        }
        Ok(rcx)
    }
}
