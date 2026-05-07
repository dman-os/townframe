use crate::interlude::*;

use crate::app::*;

use am_utils_rs::partition::PartitionStore;
use am_utils_rs::repo::BigDocHandle;
use daybook_types::doc::{UserPath, UserPathBuf};
use fs4::fs_std::FileExt;

const REPO_MARKER_FILE: &str = "db.repo.txt";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoLayout {
    pub repo_root: PathBuf,
    pub samod_root: PathBuf,
    pub sqlite_path: PathBuf,
    pub blobs_root: PathBuf,
    pub marker_path: PathBuf,
    pub lock_path: PathBuf,
}

#[derive(Debug, Clone, Default)]
pub struct RepoOpenOptions {}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct RepoLockInfo {
    pid: u32,
    created_at_unix_secs: i64,
}

pub struct RepoLockGuard {
    _file: std::fs::File,
    _path: PathBuf,
}

impl RepoLockGuard {
    pub fn acquire(lock_path: &std::path::Path) -> Res<Self> {
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut _file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(lock_path)
            .wrap_err_with(|| format!("error opening repo lock file {}", lock_path.display()))?;

        // NOTE: lock is released when file is dropped
        _file.try_lock_exclusive().map_err(|err| {
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
        _file.set_len(0)?;
        let json = serde_json::to_string(&lock_info)?;
        std::io::Write::write_all(&mut _file, json.as_bytes())?;
        std::io::Write::flush(&mut _file)?;
        Ok(Self {
            _file,
            _path: lock_path.to_path_buf(),
        })
    }
}

pub struct RepoCtx {
    pub layout: RepoLayout,
    pub lock_guard: RepoLockGuard,

    pub sql: SqlCtx,
    pub partition_store: Arc<PartitionStore>,
    partition_store_stop: am_utils_rs::partition::PartitionStoreStopToken,

    pub big_repo: SharedBigRepo,
    big_repo_stop: std::sync::Mutex<Option<am_utils_rs::BigRepoStopToken>>,

    pub doc_app: BigDocHandle,
    pub doc_drawer: BigDocHandle,

    pub local_peer_key: am_utils_rs::sync::protocol::PeerKey,
    pub local_actor_id: automerge::ActorId,
    pub local_user_path: UserPathBuf,
    pub local_device_name: String,
    pub repo_id: String,
    pub checkout_id: String,
    pub repo_name: String,

    pub iroh_public_key: String,
    pub iroh_secret_key: iroh::SecretKey,
    pub secret_repo: Arc<crate::secrets::SecretRepo>,
}

pub(crate) struct RepoCtxParts {
    pub layout: RepoLayout,
    pub lock_guard: RepoLockGuard,
    pub sql: SqlCtx,
    pub partition_store: Arc<PartitionStore>,
    pub partition_store_stop: am_utils_rs::partition::PartitionStoreStopToken,
    pub big_repo: SharedBigRepo,
    pub big_repo_stop: std::sync::Mutex<Option<am_utils_rs::BigRepoStopToken>>,
    pub local_peer_key: am_utils_rs::sync::protocol::PeerKey,
    pub local_actor_id: automerge::ActorId,
    pub local_user_path: UserPathBuf,
    pub local_device_name: String,
    pub repo_id: String,
    pub checkout_id: String,
    pub repo_name: String,
    pub iroh_public_key: String,
    pub iroh_secret_key: iroh::SecretKey,
    pub secret_repo: Arc<crate::secrets::SecretRepo>,
}

impl RepoCtx {
    pub(crate) fn from_parts(
        parts: RepoCtxParts,
        doc_app: BigDocHandle,
        doc_drawer: BigDocHandle,
    ) -> Arc<Self> {
        Arc::new(Self {
            secret_repo: parts.secret_repo,
            local_peer_key: parts.local_peer_key,
            repo_name: parts.repo_name,
            layout: parts.layout,
            lock_guard: parts.lock_guard,
            sql: parts.sql,
            partition_store: parts.partition_store,
            partition_store_stop: parts.partition_store_stop,
            big_repo: parts.big_repo,
            big_repo_stop: parts.big_repo_stop,
            doc_app,
            doc_drawer,
            local_actor_id: parts.local_actor_id,
            local_user_path: parts.local_user_path,
            repo_id: parts.repo_id,
            checkout_id: parts.checkout_id,
            iroh_public_key: parts.iroh_public_key,
            iroh_secret_key: parts.iroh_secret_key,
            local_device_name: parts.local_device_name,
        })
    }

    pub async fn shutdown(self: Arc<Self>) -> Res<()> {
        match Arc::try_unwrap(self) {
            Ok(self2) => {
                drop(self2.doc_app);
                drop(self2.doc_drawer);

                let stop = self2
                    .big_repo_stop
                    .lock()
                    .expect(ERROR_MUTEX)
                    .take()
                    .expect("big repo stop token missing, double shutdown!");
                stop.stop().await?;
            }
            Err(self2) => {
                warn!("someone is still holding on to the RepoCtx, shutdown order bug lurks!");
                let stop = self2
                    .big_repo_stop
                    .lock()
                    .expect(ERROR_MUTEX)
                    .take()
                    .expect("big repo stop token missing, double shutdown!");
                stop.stop().await?;
            }
        }
        Ok(())
    }

    pub async fn open(
        repo_root: &std::path::Path,
        _options: RepoOpenOptions,
        local_device_name: String,
    ) -> Res<Arc<Self>> {
        let layout = repo_layout(repo_root)?;
        let lock_guard = RepoLockGuard::acquire(&layout.lock_path)?;
        if !is_repo_initialized(&layout.repo_root).await? {
            eyre::bail!(
                "repo not initialized at path {} (missing marker {})",
                layout.repo_root.display(),
                layout.marker_path.display()
            );
        }
        let secret_repo = Arc::new(crate::secrets::SecretRepo::boot().await?);
        cleanup_blobs_staging_dir(&layout.blobs_root).await?;
        let sql = SqlCtx::new(SqlConfig {
            database_url: format!("sqlite://{}", layout.sqlite_path.display()),
        })
        .await?;
        let (repo_id, repo_name, user_id, checkout_id) = tokio::try_join!(
            globals::get_string_global(&sql, "global.repo_id"),
            globals::get_string_global(&sql, "global.repo_name"),
            globals::get_string_global(&sql, "global.user_id"),
            globals::get_string_global(&sql, "global.checkout_id"),
        )?;
        let repo_id = repo_id.ok_or_eyre("missing global from repo: repo_id")?;
        let repo_name = repo_name.ok_or_eyre("missing global from repo: repo_name")?;
        let user_id = user_id.ok_or_eyre("missing global from repo: user_id")?;
        let checkout_id = checkout_id.ok_or_eyre("missing global from repo: checkout_id")?;
        let identity = secret_repo
            .load_identity(&checkout_id)
            .await?
            .ok_or_eyre("missing secret from keyring")?;
        let UserInfo {
            local_peer_key,
            local_user_path,
            local_actor_id,
        } = compute_user_info(&repo_id, &user_id, &identity);
        let (partition_store, partition_store_stop) =
            PartitionStore::boot(sql.db_pool.clone()).await?;
        let (big_repo, big_repo_stop) =
            boot_big_repo(&layout, &identity, Arc::clone(&partition_store)).await?;
        let (doc_app, doc_drawer) = load_core_docs(&big_repo, &sql).await?;
        ensure_expected_partitions_for_docs(
            &partition_store,
            doc_app.document_id(),
            doc_drawer.document_id(),
        )
        .await?;
        let parts = RepoCtxParts {
            layout,
            lock_guard,
            sql,
            partition_store,
            big_repo,
            partition_store_stop,
            big_repo_stop: std::sync::Mutex::new(Some(big_repo_stop)),
            local_peer_key,
            local_actor_id,
            local_user_path,
            local_device_name,
            repo_id,
            checkout_id,
            repo_name,
            iroh_public_key: identity.iroh_public_key.to_string(),
            iroh_secret_key: identity.iroh_secret_key,
            secret_repo,
        };
        Ok(Self::from_parts(parts, doc_app, doc_drawer))
    }

    pub async fn init(
        repo_root: &std::path::Path,
        _options: RepoOpenOptions,
        repo_name: String,
        local_device_name: String,
    ) -> Res<Arc<Self>> {
        let layout = repo_layout(repo_root)?;
        let lock_guard = RepoLockGuard::acquire(&layout.lock_path)?;
        if is_repo_initialized(&layout.repo_root).await? {
            eyre::bail!(
                "repo already initialized at path {}",
                layout.repo_root.display()
            );
        }
        let secret_repo = Arc::new(crate::secrets::SecretRepo::boot().await?);
        cleanup_blobs_staging_dir(&layout.blobs_root).await?;
        let sql = SqlCtx::new(SqlConfig {
            database_url: format!("sqlite://{}", layout.sqlite_path.display()),
        })
        .await?;
        let repo_id = {
            let id = Uuid::new_v4();
            let id = utils_rs::hash::encode_base58_multibase(id);
            format!("drepo_{id}")
        };
        let checkout_id = {
            let id = Uuid::new_v4();
            let id = utils_rs::hash::encode_base58_multibase(id);
            format!("dcheckout_{id}")
        };
        let user_id = format!(
            "{}{}",
            daybook_types::doc::user_path::USER_ID_PREFIX,
            Uuid::new_v4().bs58()
        );
        tokio::try_join!(
            globals::set_string_global(&sql, "global.repo_id", &repo_id),
            globals::set_string_global(&sql, "global.checkout_id", &checkout_id),
            globals::set_string_global(&sql, "global.repo_name", &repo_name),
            globals::set_string_global(&sql, "global.user_id", &user_id),
        )?;
        let identity = {
            let secret = iroh::SecretKey::generate(&mut rand::rng());
            secret_repo.set_identity(&checkout_id, secret).await?
        };
        let UserInfo {
            local_peer_key,
            local_user_path,
            local_actor_id,
        } = compute_user_info(&repo_id, &user_id, &identity);
        let (partition_store, partition_store_stop) =
            PartitionStore::boot(sql.db_pool.clone()).await?;
        let (big_repo, big_repo_stop) =
            boot_big_repo(&layout, &identity, Arc::clone(&partition_store)).await?;
        let (doc_app, doc_drawer) = init_core_docs(&big_repo, &sql).await?;
        ensure_expected_partitions_for_docs(
            &partition_store,
            doc_app.document_id(),
            doc_drawer.document_id(),
        )
        .await?;
        Self::run_repo_init_dance(
            &big_repo,
            &partition_store,
            &doc_app,
            &doc_drawer,
            &local_user_path,
            &sql,
            layout.blobs_root.clone(),
        )
        .await?;
        mark_repo_initialized(&layout.repo_root).await?;
        let parts = RepoCtxParts {
            layout,
            lock_guard,
            sql,
            partition_store,
            partition_store_stop,
            big_repo,
            big_repo_stop: std::sync::Mutex::new(Some(big_repo_stop)),
            local_peer_key,
            local_actor_id,
            local_user_path,
            local_device_name,
            repo_id,
            checkout_id,
            repo_name,
            iroh_public_key: identity.iroh_public_key.to_string(),
            iroh_secret_key: identity.iroh_secret_key,
            secret_repo,
        };
        Ok(Self::from_parts(parts, doc_app, doc_drawer))
    }

    async fn run_repo_init_dance(
        big_repo: &SharedBigRepo,
        partition_store: &Arc<PartitionStore>,
        doc_app: &BigDocHandle,
        doc_drawer: &BigDocHandle,
        local_user_path: &UserPath,
        sql: &SqlCtx,
        blobs_root: PathBuf,
    ) -> Res<()> {
        use crate::blobs::BlobsRepo;
        use crate::config::ConfigRepo;
        use crate::drawer::DrawerRepo;
        use crate::plugs::PlugsRepo;
        use crate::rt::dispatch::DispatchRepo;
        use crate::tables::TablesRepo;

        let blobs_repo = BlobsRepo::new(
            blobs_root.clone(),
            local_user_path.to_owned(),
            Arc::new(crate::blobs::PartitionStoreMembershipWriter::new(
                Arc::clone(partition_store),
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
                *doc_app.document_id(),
                local_user_path.to_owned(),
            )
            .await
            .wrap_err("error loading plugs repo during init dance")?;
            plugs_repo = Some(repo);
            plugs_stop = Some(stop);

            let (config_repo, stop) = ConfigRepo::load(
                Arc::clone(big_repo),
                *doc_app.document_id(),
                Arc::clone(plugs_repo.as_ref().expect("plugs repo must be loaded")),
                local_user_path.to_owned(),
                sql.clone(),
            )
            .await?;
            config_stop = Some(stop);

            let config_user_path =
                daybook_types::doc::user_path::for_repo(local_user_path.to_owned(), "config-repo")?;
            let config_actor_id = daybook_types::doc::user_path::to_actor_id(&config_user_path);
            config_repo
                .upsert_actor_user_path(config_actor_id, config_user_path)
                .await?;

            let plugs_user_path =
                daybook_types::doc::user_path::for_repo(local_user_path.to_owned(), "plugs-repo")?;
            let plugs_actor_id = daybook_types::doc::user_path::to_actor_id(&plugs_user_path);
            config_repo
                .upsert_actor_user_path(plugs_actor_id, plugs_user_path)
                .await?;

            let (_tables_repo, stop) = TablesRepo::load(
                Arc::clone(big_repo),
                *doc_app.document_id(),
                local_user_path.to_owned(),
            )
            .await?;
            tables_stop = Some(stop);
            let tables_user_path =
                daybook_types::doc::user_path::for_repo(local_user_path.to_owned(), "tables-repo")?;
            let tables_actor_id = daybook_types::doc::user_path::to_actor_id(&tables_user_path);
            config_repo
                .upsert_actor_user_path(tables_actor_id, tables_user_path)
                .await?;

            let (_dispatch_repo, stop) = DispatchRepo::load(
                Arc::clone(big_repo),
                *doc_app.document_id(),
                UserPathBuf::from(local_user_path.to_string()),
                sql.clone(),
            )
            .await?;
            dispatch_stop = Some(stop);
            let dispatch_user_path = daybook_types::doc::user_path::for_repo(
                local_user_path.to_owned(),
                "dispatch-repo",
            )?;
            let dispatch_actor_id = daybook_types::doc::user_path::to_actor_id(&dispatch_user_path);
            config_repo
                .upsert_actor_user_path(dispatch_actor_id, dispatch_user_path)
                .await?;

            let (_drawer_repo, stop) = DrawerRepo::load(
                Arc::clone(big_repo),
                Arc::clone(partition_store),
                *doc_drawer.document_id(),
                UserPathBuf::from(local_user_path.to_string()),
                sql.clone(),
                blobs_root
                    .parent()
                    .ok_or_eyre("blobs root missing parent")?
                    .join("local_state"),
                Arc::new(surelock::mutex::Mutex::new(
                    crate::drawer::lru::KeyedLruPool::new(1000),
                )),
                Arc::new(surelock::mutex::Mutex::new(
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
            let drawer_user_path =
                daybook_types::doc::user_path::for_repo(local_user_path.to_owned(), "drawer-repo")?;
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

struct UserInfo {
    local_peer_key: am_utils_rs::sync::protocol::PeerKey,
    local_user_path: UserPathBuf,
    local_actor_id: automerge::ActorId,
}

fn compute_user_info(
    _repo_id: &str,
    user_id: &str,
    identity: &crate::secrets::RepoIdentity,
) -> UserInfo {
    let pkey_bs58 = utils_rs::hash::encode_base58_multibase(identity.iroh_public_key.as_bytes());
    let device_id = format!(
        "{}{}",
        daybook_types::doc::user_path::DEVICE_ID_PREFIX,
        pkey_bs58
    );
    let local_user_path = UserPathBuf::new().join("/").join(user_id).join(device_id);
    let local_peer_key = daybook_types::doc::format_peer_key(identity.iroh_public_key.as_bytes());
    let local_actor_id =
        daybook_types::doc::user_path::to_actor_id(&UserPathBuf::from(local_user_path.clone()));
    UserInfo {
        local_peer_key,
        local_user_path,
        local_actor_id,
    }
}

async fn boot_big_repo(
    layout: &RepoLayout,
    identity: &crate::secrets::RepoIdentity,
    partition_store: Arc<PartitionStore>,
) -> Res<(SharedBigRepo, am_utils_rs::BigRepoStopToken)> {
    let am_config = am_utils_rs::repo::Config {
        storage: am_utils_rs::repo::StorageConfig::Disk {
            path: layout.samod_root.clone(),
        },
        peer_id: identity.iroh_public_key.into(),
        secret_key_bytes: identity.iroh_secret_key.to_bytes(),
    };
    let (big_repo, big_repo_stop) = am_utils_rs::BigRepo::boot(am_config, partition_store).await?;
    Ok((big_repo, big_repo_stop))
}

async fn cleanup_blobs_staging_dir(blobs_root: &Path) -> Res<()> {
    let staging_root = blobs_root.join("staging");
    if tokio::fs::try_exists(&staging_root).await? {
        tokio::fs::remove_dir_all(&staging_root).await?;
    }
    tokio::fs::create_dir_all(&staging_root).await?;
    Ok(())
}

pub(crate) async fn finish_clone_init(
    parts: RepoCtxParts,
    blobs_root: PathBuf,
) -> Res<Arc<RepoCtx>> {
    let sql = &parts.sql;
    let local_user_path = &parts.local_user_path;
    let init_state = globals::get_init_state(sql).await?;
    let (doc_id_app, doc_id_drawer) = match init_state {
        globals::InitState::Created {
            doc_id_app,
            doc_id_drawer,
        } => (doc_id_app, doc_id_drawer),
        globals::InitState::None => eyre::bail!("clone init: InitState not set"),
    };
    let doc_app = parts
        .big_repo
        .get_doc(&doc_id_app)
        .await?
        .ok_or_eyre("clone init: app doc missing from BigRepo")?;
    let doc_drawer = parts
        .big_repo
        .get_doc(&doc_id_drawer)
        .await?
        .ok_or_eyre("clone init: drawer doc missing from BigRepo")?;
    ensure_expected_partitions_for_docs(&parts.partition_store, &doc_id_app, &doc_id_drawer)
        .await?;
    RepoCtx::run_repo_init_dance(
        &parts.big_repo,
        &parts.partition_store,
        &doc_app,
        &doc_drawer,
        local_user_path,
        sql,
        blobs_root,
    )
    .await?;
    Ok(RepoCtx::from_parts(parts, doc_app, doc_drawer))
}

pub(crate) async fn ensure_expected_partitions_for_docs(
    partition_store: &Arc<PartitionStore>,
    doc_app_id: &DocumentId,
    doc_drawer_id: &DocumentId,
) -> Res<()> {
    for partition_id in [
        crate::drawer::DrawerRepo::replicated_partition_id_for_drawer(doc_drawer_id),
        crate::sync::CORE_DOCS_PARTITION_ID.into(),
        crate::blobs::BLOB_SCOPE_DOCS_PARTITION_ID.into(),
        crate::blobs::BLOB_SCOPE_PLUGS_PARTITION_ID.into(),
        crate::rt::PROCESSOR_RUNLOG_PARTITION_ID.into(),
    ] {
        partition_store.ensure_partition(&partition_id).await?;
    }
    partition_store
        .upsert_item(
            doc_drawer_id.to_string().into(),
            &serde_json::json!({}),
            &[crate::sync::CORE_DOCS_PARTITION_ID.into()],
        )
        .await?;
    partition_store
        .upsert_item(
            doc_app_id.to_string().into(),
            &serde_json::json!({}),
            &[crate::sync::CORE_DOCS_PARTITION_ID.into()],
        )
        .await?;
    Ok(())
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

pub async fn is_repo_bootstrapped(repo_root: &std::path::Path) -> Res<bool> {
    if !is_repo_initialized(repo_root).await? {
        return Ok(false);
    }
    let layout = repo_layout(repo_root)?;
    if !layout.sqlite_path.exists() {
        return Ok(false);
    }
    let sql = SqlCtx::new(SqlConfig {
        database_url: format!("sqlite://{}", layout.sqlite_path.display()),
    })
    .await
    .wrap_err_with(|| {
        format!(
            "failed opening repo sqlite while checking bootstrap state: {}",
            layout.sqlite_path.display()
        )
    })?;
    let init_state = globals::get_init_state(&sql).await?;
    Ok(matches!(init_state, globals::InitState::Created { .. }))
}

async fn load_core_docs(
    big_repo: &SharedBigRepo,
    repo_sql: &SqlCtx,
) -> Res<(BigDocHandle, BigDocHandle)> {
    let init_state = globals::get_init_state(repo_sql).await?;
    let globals::InitState::Created {
        doc_id_app,
        doc_id_drawer,
    } = init_state
    else {
        eyre::bail!("repo init_state missing for existing repository");
    };
    let (handle_app, handle_drawer) = tokio::try_join!(
        big_repo.get_doc(&doc_id_app),
        big_repo.get_doc(&doc_id_drawer)
    )?;
    if handle_app.is_none() || handle_drawer.is_none() {
        eyre::bail!(
            "required core docs missing in existing repository (app_present={}, drawer_present={})",
            handle_app.is_some(),
            handle_drawer.is_some()
        );
    }
    Ok((
        handle_app.expect("checked handle_app"),
        handle_drawer.expect("checked handle_drawer"),
    ))
}

async fn init_core_docs(
    big_repo: &SharedBigRepo,
    repo_sql: &SqlCtx,
) -> Res<(BigDocHandle, BigDocHandle)> {
    use automerge::transaction::Transactable;

    let app_doc = {
        let bytes = version_updates::version_latest()?;
        let doc = automerge::Automerge::load(&bytes)
            .wrap_err("error loading version_latest for app doc")?;
        big_repo.put_doc(DocumentId::random(), doc).await?
    };
    let drawer_doc = {
        let mut doc = automerge::AutoCommit::new();
        doc.put(automerge::ROOT, "version", "0")?;
        let bytes = doc.save_nocompress();
        let doc = automerge::Automerge::load(&bytes)
            .wrap_err("error loading version_latest for drawer doc")?;
        big_repo.put_doc(DocumentId::random(), doc).await?
    };
    globals::set_init_state(
        repo_sql,
        &globals::InitState::Created {
            doc_id_app: *app_doc.document_id(),
            doc_id_drawer: *drawer_doc.document_id(),
        },
    )
    .await?;
    Ok((app_doc, drawer_doc))
}

pub mod globals {
    use crate::{app::SqlCtx, interlude::*};

    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
    pub enum InitState {
        None,
        Created {
            doc_id_app: DocumentId,
            doc_id_drawer: DocumentId,
        },
    }
    const INIT_STATE_KEY: &str = "global.init_state";

    pub async fn get_init_state(sql: &SqlCtx) -> Res<InitState> {
        let rec = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
            .bind(INIT_STATE_KEY)
            .fetch_optional(&sql.db_pool)
            .await?;
        let state = match rec {
            Some(json) => serde_json::from_str::<InitState>(&json)?,
            None => InitState::None,
        };
        Ok(state)
    }
    pub async fn set_init_state(sql: &SqlCtx, state: &InitState) -> Res<()> {
        let json = serde_json::to_string(state)?;
        sqlx::query("INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
        .bind(INIT_STATE_KEY)
        .bind(&json)
        .execute(&sql.db_pool)
        .await?;
        Ok(())
    }

    pub async fn get_string_global(sql: &SqlCtx, key: &str) -> Res<Option<String>> {
        let rec = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
            .bind(key)
            .fetch_optional(&sql.db_pool)
            .await?;
        Ok(rec)
    }

    pub async fn set_string_global(sql: &SqlCtx, key: &str, value: &str) -> Res<()> {
        let mut tx = sql.db_pool.begin().await?;

        if let Some(existing) =
            sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
                .bind(key)
                .fetch_optional(&mut *tx)
                .await?
        {
            eyre::bail!("{key} already set: {existing}");
        }

        sqlx::query("INSERT INTO kvstore(key, value) VALUES (?1, ?2)")
            .bind(key)
            .bind(value)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_string_global(sql: &SqlCtx, key: &str, value: &str) -> Res<()> {
        sqlx::query(
            "INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(value)
        .execute(&sql.db_pool)
        .await?;
        Ok(())
    }
    const SYNC_CONFIG_KEY: &str = "global.sync_config";
    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Default)]
    pub struct SyncConfig {
        pub known_devices: Vec<SyncDeviceEntry>,
    }

    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
    pub struct SyncDeviceEntry {
        pub endpoint_id: iroh::EndpointId,
        pub name: String,
        pub added_at: Timestamp,
        pub last_connected_at: Option<Timestamp>,
    }
    pub async fn get_sync_config(sql: &SqlCtx) -> Res<SyncConfig> {
        let rec = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
            .bind(SYNC_CONFIG_KEY)
            .fetch_optional(&sql.db_pool)
            .await?;
        let state = match rec {
            Some(json) => serde_json::from_str::<SyncConfig>(&json)?,
            None => SyncConfig::default(),
        };
        Ok(state)
    }

    pub async fn set_sync_config(sql: &SqlCtx, state: &SyncConfig) -> Res<()> {
        let json = serde_json::to_string(state)?;
        sqlx::query("INSERT INTO kvstore(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
            .bind(SYNC_CONFIG_KEY)
            .bind(&json)
            .execute(&sql.db_pool)
            .await?;
        Ok(())
    }
}
