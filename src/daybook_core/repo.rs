use crate::interlude::*;

use crate::app::*;

use am_utils_rs::partition::PartitionStore;
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

        // NOTE: lock is released when file is dropped
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

pub struct RepoCtx {
    pub layout: RepoLayout,
    pub lock_guard: RepoLockGuard,

    pub sql: SqlCtx,
    pub partition_store: Arc<PartitionStore>,

    pub big_repo: SharedBigRepo,
    big_repo_stop: std::sync::Mutex<Option<am_utils_rs::BigRepoStopToken>>,

    pub doc_app: am_utils_rs::repo::BigDocHandle,
    pub doc_drawer: am_utils_rs::repo::BigDocHandle,

    pub local_peer_key: am_utils_rs::sync::protocol::PeerKey,
    pub local_actor_id: automerge::ActorId,
    pub local_user_path: String,
    pub local_device_name: String,
    pub repo_id: String,
    pub repo_name: String,

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
        Self::open_inner(layout, lock_guard, options, local_device_name, false, None).await
    }

    pub async fn init(
        repo_root: &std::path::Path,
        options: RepoOpenOptions,
        repo_name: String,
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
        Self::open_inner(
            layout,
            lock_guard,
            options,
            local_device_name,
            true,
            Some(repo_name),
        )
        .await
    }

    async fn open_inner(
        layout: RepoLayout,
        lock_guard: RepoLockGuard,
        _options: RepoOpenOptions,
        local_device_name: String,
        initialize_repo: bool,
        repo_name: Option<String>,
    ) -> Res<Self> {
        // cleanup_blobs_staging_dir
        {
            let staging_root = (&layout.blobs_root).join("staging");
            if tokio::fs::try_exists(&staging_root).await? {
                tokio::fs::remove_dir_all(&staging_root).await?;
            }
            tokio::fs::create_dir_all(&staging_root).await?;
        }

        let sql = SqlCtx::new(SqlConfig {
            database_url: format!("sqlite://{}", layout.sqlite_path.display()),
        })
        .await?;
        const REPO_NAME_KEY: &str = "global.repo_name";
        const REPO_ID_KEY: &str = "global.repo_id";
        const REPO_USER_ID_KEY: &str = "global.user_id";
        let (repo_id, repo_name, repo_user_id) = if initialize_repo {
            let repo_id = {
                let id = Uuid::new_v4();
                let id = utils_rs::hash::encode_base58_multibase(id);
                format!("drepo_{id}")
            };
            let Some(repo_name) = repo_name else {
                eyre::bail!("repo name must be set when initialize_repo");
            };
            let reop_user_id = format!(
                "{}{}",
                daybook_types::doc::user_path::USER_ID_PREFIX,
                Uuid::new_v4().bs58()
            );
            tokio::try_join!(
                globals::set_string_global(&sql.db_pool, REPO_ID_KEY, &repo_id),
                globals::set_string_global(&sql.db_pool, REPO_NAME_KEY, &repo_name),
                globals::set_string_global(&sql.db_pool, REPO_USER_ID_KEY, &reop_user_id),
            )?;
            (repo_id, repo_name, reop_user_id)
        } else {
            let (repo_id, repo_name, reop_user_id) = tokio::try_join!(
                globals::get_string_global(&sql.db_pool, REPO_ID_KEY),
                globals::get_string_global(&sql.db_pool, REPO_NAME_KEY),
                globals::get_string_global(&sql.db_pool, REPO_USER_ID_KEY),
            )?;
            (
                repo_id.ok_or_eyre("missing global from repo")?,
                repo_name.ok_or_eyre("missig global from repo")?,
                reop_user_id.ok_or_eyre("missing global from repo")?,
            )
        };
        let identity = if initialize_repo {
            let secret = iroh::SecretKey::generate(&mut rand::rng());
            crate::secrets::SecretRepo::set_identity(&repo_id, secret).await?
        } else {
            crate::secrets::SecretRepo::load_identity(&repo_id)
                .await?
                .ok_or_eyre("missing secret from keyring")?
        };
        let iroh_public_key = identity.iroh_public_key.to_string();

        let local_peer_key =
            daybook_types::doc::format_peer_key(&repo_id, identity.iroh_public_key.as_bytes());
        let pkey_bs58 =
            utils_rs::hash::encode_base58_multibase(identity.iroh_public_key.as_bytes());
        let local_peer_key = format!("/{repo_id}/{pkey_bs58}").into();
        let device_id = format!(
            "{}{}",
            daybook_types::doc::user_path::DEVICE_ID_PREFIX,
            pkey_bs58
        );

        let local_user_path = format!("/{repo_user_id}/{device_id}");
        let am_config = am_utils_rs::repo::Config {
            storage: am_utils_rs::repo::StorageConfig::Disk {
                path: layout.samod_root.clone(),
            },
            peer_id: identity.iroh_public_key.into(),
        };
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(
            &daybook_types::doc::UserPath::from(local_user_path.clone()),
        );

        let (big_repo, big_repo_stop) = am_utils_rs::BigRepo::boot(am_config).await?;
        let partition_store = big_repo.partition_store();

        let (doc_app, doc_drawer) = if initialize_repo {
            init_core_docs(&big_repo, &sql.db_pool).await?
        } else {
            load_core_docs(&big_repo, &sql.db_pool).await?
        };
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
            local_peer_key,
            repo_name,
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
            repo_id,
            iroh_public_key,
            iroh_secret_key: identity.iroh_secret_key,
            local_device_name,
        })
    }

    async fn run_repo_init_dance(
        big_repo: &SharedBigRepo,
        doc_app: &am_utils_rs::repo::BigDocHandle,
        doc_drawer: &am_utils_rs::repo::BigDocHandle,
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
                sql.clone(),
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
    let init_state = globals::get_init_state(&sql.db_pool).await?;
    Ok(matches!(init_state, globals::InitState::Created { .. }))
}

async fn load_core_docs(
    big_repo: &SharedBigRepo,
    repo_sql: &SqlitePool,
) -> Res<(
    am_utils_rs::repo::BigDocHandle,
    am_utils_rs::repo::BigDocHandle,
)> {
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
    repo_sql: &SqlitePool,
) -> Res<(
    am_utils_rs::repo::BigDocHandle,
    am_utils_rs::repo::BigDocHandle,
)> {
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
            doc_id_app: app_doc.document_id().clone(),
            doc_id_drawer: drawer_doc.document_id().clone(),
        },
    )
    .await?;
    Ok((app_doc, drawer_doc))
}

pub mod globals {
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
    const INIT_STATE_KEY: &str = "global.init_state";

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

    pub async fn get_string_global(sql: &SqlitePool, key: &str) -> Res<Option<String>> {
        let rec = sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
            .bind(key)
            .fetch_optional(sql)
            .await?;
        Ok(rec)
    }

    pub async fn set_string_global(sql: &SqlitePool, key: &str, value: &str) -> Res<()> {
        let mut tx = sql.begin().await?;

        if let Some(repo_id) =
            sqlx::query_scalar::<_, String>("SELECT value FROM kvstore WHERE key = ?1")
                .bind(key)
                .fetch_optional(&mut *tx)
                .await?
        {
            eyre::bail!("{key} already set: {repo_id}");
        }

        sqlx::query("INSERT INTO kvstore(key, value) VALUES (?1, ?2)")
            .bind(key)
            .bind(value)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
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
