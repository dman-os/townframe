use crate::interlude::*;
use automerge::transaction::Transactable;

use big_repo::{BigRepo, SharedBigRepo};
use sqlx::sqlite::SqliteConnectOptions;
use std::str::FromStr;

use crate::drawer::DrawerRepo;
use crate::plugs::PlugsRepo;

pub struct DaybookTestContext {
    pub _acx: SharedBigRepo,
    pub big_sync_stop: big_sync::StopToken,
    pub drawer_repo: Arc<DrawerRepo>,
    pub dispatch_repo: Arc<crate::rt::dispatch::DispatchRepo>,
    pub _config_repo: Arc<crate::config::ConfigRepo>,
    pub drawer_stop: crate::repos::RepoStopToken,
    pub plugs_stop: crate::repos::RepoStopToken,
    pub config_stop: crate::repos::RepoStopToken,
    pub dispatch_stop: crate::repos::RepoStopToken,
    pub progress_stop: crate::repos::RepoStopToken,
    pub init_stop: crate::repos::RepoStopToken,
    pub sqlite_local_state_stop: crate::repos::RepoStopToken,
    pub rt_stop: crate::rt::RtStopToken,
    pub rt: Arc<crate::rt::Rt>,
    pub _temp_dir: tempfile::TempDir,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DaybookTestCxOptions {
    pub provision_mltools_models: bool,
}

impl DaybookTestContext {
    /// Wait until there are no active jobs, with a timeout
    pub async fn _wait_until_no_active_jobs(&self, timeout_secs: u64) -> Res<()> {
        use tokio::time::{Duration, Instant};

        let start = Instant::now();
        let timeout_duration = Duration::from_secs(timeout_secs);
        let mut change_rx = self.rt.wflow_part_state.change_receiver();

        // Get initial counts without holding a lock
        let mut counts = *change_rx.borrow();
        if counts.active == 0 && counts.archive > 0 {
            // No active jobs, we're done
            tracing::info!(
                "done, {} active jobs, {} archived jobs",
                counts.active,
                counts.archive
            );
            return Ok(());
        }

        loop {
            // Calculate remaining time
            let elapsed = start.elapsed();
            let remaining = timeout_duration.saturating_sub(elapsed);
            if remaining.is_zero() {
                return Err(ferr!(
                    "timeout waiting for no active jobs after {} seconds (elapsed: {:?}, active jobs: {})",
                    timeout_secs,
                    elapsed,
                    counts.active
                ));
            }

            tracing::debug!(
                "Waiting for count change or timeout (active jobs: {}, remaining: {:?})",
                counts.active,
                remaining
            );

            // Wait for the next count change or timeout
            match tokio::time::timeout(remaining, change_rx.changed()).await {
                Ok(Ok(())) => {
                    // Counts changed, update our local copy
                    counts = *change_rx.borrow();
                    if counts.active == 0 && counts.archive > 0 {
                        // No active jobs, we're done
                        tracing::info!(
                            "done, {} active jobs, {} archived jobs",
                            counts.active,
                            counts.archive
                        );
                        return Ok(());
                    }
                    // Continue waiting
                    tracing::debug!("Counts changed, rechecking active jobs");
                    continue;
                }
                Ok(Err(_)) => {
                    // Channel closed, worker might be shutting down
                    return Err(ferr!("worker state channel closed"));
                }
                Err(_) => {
                    // Timeout reached
                    let final_elapsed = start.elapsed();
                    let final_counts = self.rt.wflow_part_state.get_job_counts().await;
                    return Err(ferr!(
                        "timeout waiting for no active jobs after {} seconds (elapsed: {:?}, active jobs: {})",
                        timeout_secs,
                        final_elapsed,
                        final_counts.active
                    ));
                }
            }
        }
    }

    pub async fn stop(self) -> Res<()> {
        self.rt_stop.stop().await?;
        Arc::clone(&self.rt.rcx).shutdown().await?;
        self.drawer_stop.stop().await?;
        self.progress_stop.stop().await?;
        self.dispatch_stop.stop().await?;
        self.config_stop.stop().await?;
        self.plugs_stop.stop().await?;
        self.init_stop.stop().await?;
        self.sqlite_local_state_stop.stop().await?;
        self.big_sync_stop.stop().await?;
        Ok(())
    }
}

pub async fn test_cx(test_name: &'static str) -> Res<DaybookTestContext> {
    test_cx_with_options(test_name, DaybookTestCxOptions::default()).await
}

pub async fn test_cx_with_options(
    _test_name: &'static str,
    options: DaybookTestCxOptions,
) -> Res<DaybookTestContext> {
    tokio::task::block_in_place(|| {
        utils_rs::testing::load_envs_once();
        utils_rs::testing::setup_tracing_once();
    });

    // Generate unique IDs for this test to ensure complete isolation across parallel test runs
    let device_id = format!("test_{}", uuid::Uuid::new_v4().simple());
    let peer_id = crate::peer_id_from_label(&format!("test_{}", uuid::Uuid::new_v4().simple()));

    // Initialize SharedBigRepo with memory storage
    let (big_sync_host, big_sync_stop) =
        crate::test_support::boot_part_store("sqlite::memory:").await?;
    let (big_repo, acx_stop) = BigRepo::boot(
        big_repo::Config {
            peer_id,
            secret_key_bytes: rand::random::<[u8; 32]>(),
            storage: big_repo::StorageConfig::Memory,
        },
        Arc::clone(&big_sync_host.store),
    )
    .await?;

    // Create a drawer document
    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = big_repo.put_doc(DocumentId::random(), doc).await?;
        handle.document_id()
    };

    // Create an app document for all stores (config, plugs, dispatch, triage)
    let app_doc_id = {
        let doc = automerge::Automerge::load(&crate::app::version_updates::version_latest()?)?;
        let handle = big_repo.put_doc(DocumentId::random(), doc).await?;
        handle.document_id()
    };

    // Load config first to get local identity
    let local_user_path = daybook_types::doc::UserPathBuf::from("/test-user");
    let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);
    let temp_dir = tempfile::tempdir()?;
    let part_store = Arc::clone(&big_sync_host.store);
    let blobs = crate::blobs::BlobsRepo::new(
        temp_dir.path().join("blobs"),
        local_user_path.clone(),
        Arc::new(crate::blobs::PartitionStoreMembershipWriter::new(
            Arc::clone(&part_store),
        )),
    )
    .await?;

    let (plugs_repo, plugs_stop) = PlugsRepo::load(
        Arc::clone(&big_repo),
        Arc::clone(&blobs),
        app_doc_id,
        local_user_path.clone(),
    )
    .await?;
    let sql_ctx = crate::app::SqlCtx::new(crate::app::SqlConfig {
        database_url: "sqlite::memory:".into(),
    })
    .await?;
    let (config_repo, config_stop) = crate::config::ConfigRepo::load(
        Arc::clone(&big_repo),
        app_doc_id,
        Arc::clone(&plugs_repo),
        local_user_path.clone(),
        sql_ctx.clone(),
    )
    .await?;

    let config_user_path =
        daybook_types::doc::user_path::for_repo(local_user_path.clone(), "config-repo")?;
    let config_actor_id = daybook_types::doc::user_path::to_actor_id(&config_user_path);
    config_repo
        .upsert_actor_user_path(config_actor_id, config_user_path)
        .await?;

    let plugs_user_path =
        daybook_types::doc::user_path::for_repo(local_user_path.clone(), "plugs-repo")?;
    let plugs_actor_id = daybook_types::doc::user_path::to_actor_id(&plugs_user_path);
    config_repo
        .upsert_actor_user_path(plugs_actor_id, plugs_user_path)
        .await?;
    let (dispatch_repo, dispatch_stop) = crate::rt::dispatch::DispatchRepo::load(
        Arc::clone(&big_repo),
        app_doc_id,
        local_user_path.clone(),
        sql_ctx.clone(),
    )
    .await?;
    let dispatch_user_path =
        daybook_types::doc::user_path::for_repo(local_user_path.clone(), "dispatch-repo")?;
    let dispatch_actor_id = daybook_types::doc::user_path::to_actor_id(&dispatch_user_path);
    config_repo
        .upsert_actor_user_path(dispatch_actor_id, dispatch_user_path)
        .await?;
    let (progress_repo, progress_stop) =
        crate::progress::ProgressRepo::boot(sql_ctx.clone()).await?;
    let (drawer_repo, drawer_stop) = DrawerRepo::load(
        Arc::clone(&big_repo),
        Arc::clone(&part_store),
        drawer_doc_id,
        local_user_path.clone(),
        sql_ctx.clone(),
        temp_dir.path().join("local_states"),
        Arc::new(surelock::mutex::Mutex::new(
            crate::drawer::lru::KeyedLruPool::new(1000),
        )),
        Arc::new(surelock::mutex::Mutex::new(
            crate::drawer::lru::KeyedLruPool::new(1000),
        )),
        #[cfg(not(test))]
        Arc::clone(&plugs_repo),
        #[cfg(test)]
        Some(Arc::clone(&plugs_repo)),
    )
    .await?;
    let drawer_user_path =
        daybook_types::doc::user_path::for_repo(local_user_path.clone(), "drawer-repo")?;
    let drawer_actor_id = daybook_types::doc::user_path::to_actor_id(&drawer_user_path);
    config_repo
        .upsert_actor_user_path(drawer_actor_id, drawer_user_path)
        .await?;

    if options.provision_mltools_models {
        let mltools_config = mltools::models::mobile_default(mltools::models::test_cache_dir())
            .await
            .wrap_err("error provisioning default mltools models for e2e")?;
        config_repo
            .set_mltools_config(mltools_config)
            .await
            .wrap_err("error storing e2e mltools config")?;
    }

    plugs_repo.ensure_system_plugs().await?;

    let repo_root = temp_dir.path().join("repo");
    tokio::fs::create_dir_all(&repo_root).await?;
    let layout = crate::repo::RepoLayout {
        repo_root: repo_root.clone(),
        samod_root: repo_root.join("samod"),
        sqlite_path: repo_root.join("sqlite.db"),
        blobs_root: repo_root.join("blobs"),
        marker_path: repo_root.join("db.repo.txt"),
        lock_path: repo_root.join("repo.lock"),
    };
    let lock_guard = crate::repo::RepoLockGuard::acquire(&layout.lock_path)?;
    let secret_repo = Arc::new(crate::secrets::SecretRepo::boot().await?);
    let iroh_secret_key = iroh::SecretKey::generate();
    let local_peer_key = daybook_types::doc::format_peer_key(peer_id.as_bytes());
    let rcx = crate::repo::RepoCtx::from_parts(
        crate::repo::RepoCtxParts {
            layout,
            lock_guard,
            sql: sql_ctx.clone(),
            part_store: Arc::clone(&part_store),
            big_repo: Arc::clone(&big_repo),
            big_repo_stop: std::sync::Mutex::new(Some(acx_stop)),
            local_peer_key,
            local_actor_id: local_actor_id.clone(),
            local_user_path: local_user_path.clone(),
            local_device_name: device_id.clone(),
            repo_id: format!("test-repo-{}", uuid::Uuid::new_v4().simple()),
            checkout_id: format!("test-checkout-{}", uuid::Uuid::new_v4().simple()),
            repo_name: format!("test-repo-{}", uuid::Uuid::new_v4().simple()),
            iroh_public_key: peer_id.to_string(),
            iroh_secret_key,
            secret_repo: Arc::clone(&secret_repo),
        },
        big_repo
            .get_doc(&app_doc_id)
            .await?
            .ok_or_eyre("missing app doc")?,
        big_repo
            .get_doc(&drawer_doc_id)
            .await?
            .ok_or_eyre("missing drawer doc")?,
    );

    let (init_repo, init_stop) = crate::rt::init::InitRepo::load(
        Arc::clone(&big_repo),
        app_doc_id,
        local_actor_id,
        sql_ctx.clone(),
    )
    .await?;
    let (sqlite_local_state_repo, sqlite_local_state_stop) =
        crate::local_state::SqliteLocalStateRepo::boot(temp_dir.path().join("local_states"))
            .await?;

    let (rt, rt_stop) = crate::rt::Rt::boot(
        crate::rt::RtConfig {
            device_id: device_id.clone(),
        },
        rcx,
        Arc::clone(&drawer_repo),
        Arc::clone(&plugs_repo),
        Arc::clone(&dispatch_repo),
        Arc::clone(&progress_repo),
        Arc::clone(&blobs),
        Arc::clone(&config_repo),
        init_repo,
        sqlite_local_state_repo,
    )
    .await?;

    Ok(DaybookTestContext {
        _acx: big_repo,
        big_sync_stop,
        drawer_repo,
        rt,
        dispatch_repo,
        _config_repo: config_repo,
        drawer_stop,
        plugs_stop,
        config_stop,
        dispatch_stop,
        progress_stop,
        init_stop,
        sqlite_local_state_stop,
        rt_stop,
        _temp_dir: temp_dir,
    })
}

#[cfg(test)]
pub async fn import_test_plug_oci(test_cx: &DaybookTestContext) -> Res<()> {
    let artifact_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/oci")
        .join("@daybook/test");
    eyre::ensure!(
        artifact_path.exists(),
        "missing OCI plug artifact at '{}'. Build it first with: cargo run -p xtask -- build-plug-oci --plug-root ./src/plug_test",
        artifact_path.display()
    );
    test_cx
        .rt
        .plugs_repo
        .import_from_oci_layout(&artifact_path, crate::plugs::OciImportOptions::default())
        .await?;
    Ok(())
}

pub async fn boot_part_store(sqlite_url: &str) -> Res<(big_sync::Ctx, big_sync::StopToken)> {
    let (read_pool, write_pool) = {
        let connect_options = SqliteConnectOptions::from_str(sqlite_url)
            .expect(ERROR_IMPOSSIBLE)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .create_if_missing(true);
        let read_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(connect_options.clone())
            .await
            .wrap_err("failed connecting big repo sqlite read pool")?;
        let write_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .wrap_err("failed connecting big repo sqlite write pool")?;
        (read_pool, write_pool)
    };

    let store = Arc::new(
        big_sync::SqlitePartStore::new(
            read_pool,
            write_pool,
            sqlite_url.to_owned(),
            big_sync_core::BuckId::MAX_LEVEL,
        )
        .await?,
    );
    let store: Arc<dyn big_sync::HostPartStore> = store as _;
    let (worker, stop) = big_sync::spawn_big_sync_worker(Arc::clone(&store), HashMap::new())?;
    Ok((big_sync::Ctx { store, worker }, stop))
}

pub async fn boot_repo() -> Res<(
    Arc<BigRepo>,
    big_sync::Ctx,
    Box<dyn FnOnce() -> futures::future::BoxFuture<'static, Res<()>>>,
)> {
    let (big_sync_host, big_sync_stop) = boot_part_store("sqlite::memory:").await?;
    let (repo, stop) = BigRepo::boot(
        big_repo::Config {
            peer_id: PeerId::new([7_u8; 32]),
            secret_key_bytes: [7_u8; 32],
            storage: big_repo::StorageConfig::Memory,
        },
        Arc::clone(&big_sync_host.store),
    )
    .await?;
    Ok((
        repo,
        big_sync_host,
        Box::new(move || {
            async move {
                stop.stop().await?;
                big_sync_stop.stop().await?;
                eyre::Ok(())
            }
            .boxed()
        }),
    ))
}

pub async fn boot_disk_repo(
    path: PathBuf,
) -> Res<(
    Arc<BigRepo>,
    big_sync::Ctx,
    Box<dyn FnOnce() -> futures::future::BoxFuture<'static, Res<()>>>,
)> {
    std::fs::create_dir_all(&path)
        .wrap_err_with(|| format!("failed creating disk repo path: {}", path.display()))?;
    let (big_sync_host, big_sync_stop) = boot_part_store(&format!(
        "sqlite://{}",
        path.join("part_store.db").display()
    ))
    .await?;
    let (repo, stop) = BigRepo::boot(
        big_repo::Config {
            peer_id: PeerId::new([7_u8; 32]),
            secret_key_bytes: [7_u8; 32],
            storage: big_repo::StorageConfig::Disk { path },
        },
        Arc::clone(&big_sync_host.store),
    )
    .await?;
    Ok((
        repo,
        big_sync_host,
        Box::new(move || {
            async move {
                stop.stop().await?;
                big_sync_stop.stop().await?;
                eyre::Ok(())
            }
            .boxed()
        }),
    ))
}
