use crate::interlude::*;

pub mod distributed_smoke;
pub(crate) mod sync_node;

use big_repo::{BigRepo, SharedBigRepo};

use crate::drawer::DrawerRepo;
use crate::plugs::PlugsRepo;

/// The band these harnesses run. They mirror the daybook embedder, which opts out of the
/// bucket path because bucket-diff was observed stalling in its offline-reopen scenario, so
/// its tests stay on the cursor path rather than inheriting the new default.
const HARNESS_SYNC_MODE: Option<big_sync::SyncMode> = Some(big_sync::SyncMode::CursorOnly);

pub struct DaybookTestContext {
    state: Option<DaybookTestState>,
}

pub struct DaybookTestState {
    pub _acx: SharedBigRepo,
    pub drawer_repo: Arc<DrawerRepo>,
    pub dispatch_repo: Arc<crate::rt::dispatch::DispatchRepo>,
    pub _config_repo: Arc<crate::config::ConfigRepo>,
    pub rt: Arc<crate::rt::Rt>,
    pub _temp_dir: tempfile::TempDir,
    owners: sync_node::Owners,
}

impl std::ops::Deref for DaybookTestContext {
    type Target = DaybookTestState;
    fn deref(&self) -> &Self::Target {
        self.state.as_ref().expect("stopped test context")
    }
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

    pub async fn stop(mut self) -> Res<()> {
        self.state
            .take()
            .expect("stopped test context")
            .stop()
            .await
    }
}
impl DaybookTestState {
    async fn stop(self) -> Res<()> {
        let Self {
            _acx,
            drawer_repo,
            dispatch_repo,
            _config_repo,
            rt,
            _temp_dir,
            mut owners,
        } = self;
        drop((_acx, drawer_repo, dispatch_repo, _config_repo, rt));
        owners.stop(/*runtime_first*/ false).await?;
        // Temporary files outlive their stores and all consuming workers.
        drop(_temp_dir);
        Ok(())
    }
}
impl Drop for DaybookTestContext {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(state.stop())
            })
            .expect("test context teardown failed");
        }
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

    let temp_dir = tempfile::tempdir()?;
    // Generate unique IDs for this test to ensure complete isolation across parallel test runs
    let device_id = format!("test_{}", uuid::Uuid::new_v4().simple());
    let peer_id = crate::peer_id_from_label(&format!("test_{}", uuid::Uuid::new_v4().simple()));

    // Initialize SharedBigRepo with memory storage
    let (big_repo, acx_stop) = BigRepo::boot(big_repo::Config {
        node_identity_seed: rand::random::<[u8; 32]>(),
        storage: big_repo::StorageConfig::Memory,
        scope_key: Arc::from("daybook-core-test"),
        hidden_parts: Default::default(),
        automerge_frontier_group_scope: Default::default(),
        causal_checkpoint_group_scope: Default::default(),
        group_part_group_scope: Default::default(),
    })
    .await?;
    let mut owners = sync_node::Owners::before_context(acx_stop);
    let part_store = big_repo.shared_part_store();
    let (_worker, big_sync_stop) = big_sync::spawn_big_sync_worker_with_options(
        Arc::clone(&part_store),
        HashMap::new(),
        "daybook-test-cx",
        None,
        HARNESS_SYNC_MODE,
        Arc::from("daybook-core-test"),
    )?;
    owners.big_sync = Some(big_sync_stop);

    // Create a drawer document
    let drawer_doc_id = {
        let doc = automerge::Automerge::load(&crate::drawer::version_updates::version_latest()?)?;
        let handle = big_repo.create_doc(doc).await?;
        handle.document_id()
    };

    // Create an app document for all stores (config, plugs, dispatch, triage)
    let app_doc_id = {
        let doc = automerge::Automerge::load(&crate::app::version_updates::version_latest()?)?;
        let handle = big_repo.create_doc(doc).await?;
        handle.document_id()
    };

    // ADR 007 §2: the repo config doc (third core doc) hosting the plugg
    // config facet.
    let config_doc_id = {
        let doc =
            automerge::Automerge::load(&crate::drawer::doc_version_updates::version_latest()?)?;
        let handle = big_repo.create_doc(doc).await?;
        handle.document_id()
    };

    // Load config first to get local identity
    let local_user_path = daybook_types::doc::UserPathBuf::from("/test-user");
    let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);

    let blob_part_store = crate::repo::open_blob_part_store(big_repo.sql_ctx()).await?;
    let blobs =
        crate::blobs::BlobsRepo::new(temp_dir.path().join("blobs"), local_user_path.clone())
            .await?;
    owners.blobs = Some(Arc::clone(&blobs));
    let (sqlite_local_state_repo, sqlite_local_state_stop) =
        crate::local_state::SqliteLocalStateRepo::boot(temp_dir.path().join("local_state")).await?;
    owners.local_state = Some(sqlite_local_state_stop);
    let sql_ctx = crate::app::open_sql_ctx(crate::app::SqlConfig::memory()).await?;

    let (plugs_repo, plugs_stop) = PlugsRepo::load(
        Arc::clone(&big_repo),
        Arc::clone(&blobs),
        config_doc_id.clone(),
        local_user_path.clone(),
        Arc::clone(&sqlite_local_state_repo),
    )
    .await?;
    owners.plugs = Some(plugs_stop);
    let (drawer_repo, drawer_stop) = DrawerRepo::load(
        Arc::clone(&big_repo),
        Arc::clone(&part_store),
        drawer_doc_id.clone(),
        local_user_path.clone(),
        sql_ctx.clone(),
        temp_dir.path().join("local_state"),
        Arc::new(surelock::mutex::Mutex::new(
            utils_rs::lru::KeyedLruPool::new(1000),
        )),
        Arc::new(surelock::mutex::Mutex::new(
            utils_rs::lru::KeyedLruPool::new(1000),
        )),
        #[cfg(not(test))]
        Arc::clone(&plugs_repo),
        #[cfg(test)]
        Some(Arc::clone(&plugs_repo)),
    )
    .await?;
    owners.drawer = Some(drawer_stop);
    let (config_repo, config_stop) = crate::config::ConfigRepo::load(
        Arc::clone(&big_repo),
        app_doc_id.clone(),
        Arc::clone(&plugs_repo),
        local_user_path.clone(),
        sql_ctx.clone(),
    )
    .await?;
    owners.config = Some(config_stop);

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
    let drawer_user_path =
        daybook_types::doc::user_path::for_repo(local_user_path.clone(), "drawer-repo")?;
    let drawer_actor_id = daybook_types::doc::user_path::to_actor_id(&drawer_user_path);
    config_repo
        .upsert_actor_user_path(drawer_actor_id, drawer_user_path)
        .await?;
    let (dispatch_repo, dispatch_stop) = crate::rt::dispatch::DispatchRepo::load(
        Arc::clone(&big_repo),
        app_doc_id.clone(),
        local_user_path.clone(),
        sql_ctx.clone(),
    )
    .await?;
    owners.dispatch = Some(dispatch_stop);
    let dispatch_user_path =
        daybook_types::doc::user_path::for_repo(local_user_path.clone(), "dispatch-repo")?;
    let dispatch_actor_id = daybook_types::doc::user_path::to_actor_id(&dispatch_user_path);
    config_repo
        .upsert_actor_user_path(dispatch_actor_id, dispatch_user_path)
        .await?;
    let (progress_repo, progress_stop) =
        crate::progress::ProgressRepo::boot(sql_ctx.clone()).await?;
    owners.progress = Some(progress_stop);

    if options.provision_mltools_models {
        let mltools_config = mltools::models::mobile_default(mltools::models::test_cache_dir())
            .await
            .wrap_err("error provisioning default mltools models for e2e")?;
        config_repo
            .set_mltools_config(mltools_config)
            .await
            .wrap_err("error storing e2e mltools config")?;
    }

    let repo_root = temp_dir.path().join("repo");
    tokio::fs::create_dir_all(&repo_root).await?;
    let layout = crate::repo::RepoLayout {
        repo_root: repo_root.clone(),
        big_repo_root: repo_root.join("big_repo"),
        sqlite_path: repo_root.join("sqlite.db"),
        blobs_root: repo_root.join("blobs"),
        marker_path: repo_root.join("db.repo.txt"),
        lock_path: repo_root.join("repo.lock"),
    };
    let lock_guard = crate::repo::RepoLockGuard::acquire(layout.lock_path.clone()).await?;
    let secret_store = secrets_rs::SecretStore::boot().await?;
    let iroh_secret_key = iroh::SecretKey::generate();
    let local_peer_key =
        daybook_types::doc::format_peer_key(&peer_id.to_bytes32().expect(ERROR_IMPOSSIBLE));
    let authority = crate::authority::ensure(&big_repo, &sql_ctx, None).await?;
    let core_inventory_daybook_id = drawer_repo
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: default(),
            user_path: None,
        })
        .await?;
    let docs_inventory_daybook_id = drawer_repo
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPathBuf::from("main"),
            facets: default(),
            user_path: None,
        })
        .await?;
    let core_entry = drawer_repo
        .get_entry(&core_inventory_daybook_id)
        .await?
        .ok_or_eyre("missing core inventory doc entry")?;
    let docs_entry = drawer_repo
        .get_entry(&docs_inventory_daybook_id)
        .await?
        .ok_or_eyre("missing docs inventory doc entry")?;
    let core_inventory_doc_id = core_entry
        .branches
        .get("main")
        .ok_or_eyre("missing main branch for core inventory doc")?
        .branch_doc_id
        .clone();
    let docs_inventory_doc_id = docs_entry
        .branches
        .get("main")
        .ok_or_eyre("missing main branch for docs inventory doc")?
        .branch_doc_id
        .clone();

    big_repo
        .add_admin_member_to_doc(
            core_inventory_doc_id.clone(),
            authority.blob_inventories.clone(),
        )
        .await?;
    big_repo
        .add_admin_member_to_doc(
            docs_inventory_doc_id.clone(),
            authority.blob_inventories.clone(),
        )
        .await?;

    config_repo
        .set_blob_inventories(crate::config::AppBlobInventories {
            core_inventory_doc_id: core_inventory_doc_id.clone(),
            docs_inventory_doc_id: docs_inventory_doc_id.clone(),
        })
        .await?;

    crate::authority::grant_docs_admin(
        &big_repo,
        &authority.core_docs,
        [
            app_doc_id.clone(),
            drawer_doc_id.clone(),
            core_inventory_doc_id.clone(),
            docs_inventory_doc_id.clone(),
        ],
    )
    .await?;
    crate::repo::ensure_authority_partitions(
        &part_store,
        &authority,
        &core_inventory_doc_id,
        &docs_inventory_doc_id,
    )
    .await?;
    crate::repo::ensure_blob_partitions(
        &blob_part_store,
        &core_inventory_doc_id,
        &docs_inventory_doc_id,
    )
    .await?;
    let app_doc = big_repo
        .get_doc(&app_doc_id)
        .await?
        .into_ready(app_doc_id.clone())?;
    let drawer_doc = big_repo
        .get_doc(&drawer_doc_id)
        .await?
        .into_ready(drawer_doc_id)?;
    let config_doc = big_repo
        .get_doc(&config_doc_id)
        .await?
        .into_ready(config_doc_id)?;
    let rcx = crate::repo::RepoCtx::from_parts(
        crate::repo::RepoCtxParts {
            layout,
            lock_guard,
            options: crate::repo::RepoOpenOptions::default(),
            sql: sql_ctx.clone(),
            sqlite_local_state_repo: Arc::clone(&sqlite_local_state_repo),
            sqlite_local_state_stop: std::sync::Mutex::new(owners.local_state.take()),
            part_store: Arc::clone(&part_store),
            blob_part_store: Arc::clone(&blob_part_store),
            frontier_part_store: big_repo.frontier_part_store(),
            derived_part_store: big_repo.derived_part_store(),
            big_repo: Arc::clone(&big_repo),
            big_repo_stop: std::sync::Mutex::new(owners.big_repo.take()),
            local_peer_key,
            local_actor_id: local_actor_id.clone(),
            local_user_path: local_user_path.clone(),
            local_device_name: device_id.clone(),
            repo_id: format!("test-repo-{}", uuid::Uuid::new_v4().simple()),
            checkout_id: format!("test-checkout-{}", uuid::Uuid::new_v4().simple()),
            repo_name: format!("test-repo-{}", uuid::Uuid::new_v4().simple()),
            iroh_public_key: peer_id.to_string(),
            iroh_secret_key,
            secret_store,
        },
        app_doc,
        drawer_doc,
        config_doc,
        core_inventory_doc_id,
        docs_inventory_doc_id,
    );
    owners.ctx = Some(Arc::clone(&rcx));

    let (init_repo, init_stop) = crate::rt::init::InitRepo::load(
        Arc::clone(&big_repo),
        app_doc_id,
        local_user_path.clone(),
        sql_ctx.clone(),
        Arc::clone(&progress_repo),
        None,
    )
    .await?;
    owners.init = Some(init_stop);
    let init_user_path =
        daybook_types::doc::user_path::for_repo(local_user_path.clone(), "init-repo")?;
    let init_actor_id = daybook_types::doc::user_path::to_actor_id(&init_user_path);
    config_repo
        .upsert_actor_user_path(init_actor_id, init_user_path)
        .await?;

    let (rt, rt_stop) = crate::rt::Rt::boot(
        crate::rt::RtConfig::new(device_id.clone(), /*startup_progress_task_id*/ None),
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
    owners.rt = Some(rt_stop);

    Ok(DaybookTestContext {
        state: Some(DaybookTestState {
            _acx: big_repo,
            drawer_repo,
            rt,
            dispatch_repo,
            _config_repo: config_repo,
            _temp_dir: temp_dir,
            owners,
        }),
    })
}

#[cfg(test)]
pub async fn import_test_plug_oci(test_cx: &DaybookTestContext) -> Res<crate::plugs::ImportedPlug> {
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
        .await
}

/// ADR 007: authoring imports make a plug known; runtime behavior (processors,
/// pins) requires it to be enabled. Tests that need dispatch/pin behavior must
/// import AND enable.
#[cfg(test)]
pub async fn import_and_enable_test_plug(test_cx: &DaybookTestContext) -> Res<()> {
    let imported = import_test_plug_oci(test_cx).await?;
    let doc_id = imported
        .doc_id
        .ok_or_eyre("imported test plug missing manifest doc id")?;
    let ref_url: url::Url =
        format!("db+facet:///{doc_id}/org.example.daybook.plugManifest/main?branch=main")
            .parse()?;
    let target = test_cx.rt.plugs_repo.enable_plug(&ref_url).await?;
    let status = test_cx
        .rt
        .triage_worker
        .wait_for_activation(&test_cx.rt.plugs_repo, target)
        .await?;
    eyre::ensure!(
        matches!(status, crate::rt::triage::ActivationStatus::Active(_)),
        "test plug activation failed: {status:?}"
    );
    Ok(())
}
pub async fn boot_part_store(sqlite_url: &str) -> Res<(big_sync::Ctx, big_sync::StopToken)> {
    let sql = sqlx_utils_rs::SqlCtx::url(sqlite_url).await?;

    let store = Arc::new(
        big_sync::SqlitePartStore::new(
            sql,
            sqlite_url.to_owned(),
            big_sync_core::BuckId::MAX_LEVEL,
        )
        .await?,
    );
    let store: Arc<dyn big_sync::HostPartStore> = store as _;
    let (worker, stop) = big_sync::spawn_big_sync_worker_with_options(
        Arc::clone(&store),
        HashMap::new(),
        "daybook-test-part-store",
        None,
        HARNESS_SYNC_MODE,
        Arc::from("daybook-core-test"),
    )?;
    Ok((big_sync::Ctx { store, worker }, stop))
}

pub async fn boot_repo() -> Res<(
    Arc<BigRepo>,
    big_sync::Ctx,
    Box<dyn FnOnce() -> futures::future::BoxFuture<'static, Res<()>>>,
)> {
    let (repo, stop) = BigRepo::boot(big_repo::Config {
        node_identity_seed: [7_u8; 32],
        storage: big_repo::StorageConfig::Memory,
        scope_key: Arc::from("daybook-core-test"),
        hidden_parts: Default::default(),
        automerge_frontier_group_scope: Default::default(),
        causal_checkpoint_group_scope: Default::default(),
        group_part_group_scope: Default::default(),
    })
    .await?;
    let part_store = repo.shared_part_store();
    let (worker, big_sync_stop) = big_sync::spawn_big_sync_worker_with_options(
        Arc::clone(&part_store),
        HashMap::new(),
        "daybook-boot-repo",
        None,
        HARNESS_SYNC_MODE,
        Arc::from("daybook-core-test"),
    )?;
    let big_sync_host = big_sync::Ctx {
        store: part_store,
        worker,
    };
    Ok((
        repo,
        big_sync_host,
        Box::new(move || {
            async move {
                big_sync_stop.stop().await?;
                stop.stop().await
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
    tokio::fs::create_dir_all(&path)
        .await
        .wrap_err_with(|| format!("failed creating disk repo path: {}", path.display()))?;
    let (repo, stop) = BigRepo::boot(big_repo::Config {
        node_identity_seed: [7_u8; 32],
        storage: big_repo::StorageConfig::Disk { path },
        scope_key: Arc::from("daybook-core-test"),
        hidden_parts: Default::default(),
        automerge_frontier_group_scope: Default::default(),
        causal_checkpoint_group_scope: Default::default(),
        group_part_group_scope: Default::default(),
    })
    .await?;
    let mut owners = sync_node::Owners::before_context(stop);
    let part_store = repo.shared_part_store();
    let (worker, big_sync_stop) = big_sync::spawn_big_sync_worker_with_options(
        Arc::clone(&part_store),
        HashMap::new(),
        "daybook-boot-disk",
        None,
        HARNESS_SYNC_MODE,
        Arc::from("daybook-core-test"),
    )?;
    owners.big_sync = Some(big_sync_stop);
    let big_sync_host = big_sync::Ctx {
        store: part_store,
        worker,
    };
    Ok((
        repo,
        big_sync_host,
        Box::new(move || {
            async move {
                owners.stop(/*runtime_first*/ false).await
            }
            .boxed()
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn disk_bootstrap_early_error_preserves_document_for_reopen() -> Res<()> {
        use automerge::{ReadDoc, transaction::Transactable};
        let directory = tempfile::tempdir()?;
        let root = directory.path().join("repo");
        let mut witness = None;
        let failed: Res<()> = async {
            let (repo, _sync, _stop) = boot_disk_repo(root.clone()).await?;
            let mut seed = automerge::Automerge::new();
            let mut transaction = seed.transaction();
            transaction.put(automerge::ROOT, "note", "retained after refusal")?;
            transaction.commit();
            let document = repo.create_doc(seed).await?.document_id();
            repo.wait_for_quiescence(None).await?;
            witness = Some((repo, document));
            eyre::bail!("scenario refused after durable native publication");
        }
        .await;
        let failure = failed.expect_err("exercise dropped teardown closure");
        let (stopped, document) = witness.ok_or(failure)?;
        assert!(stopped.get_doc(&document).await.is_err());
        drop(stopped);
        let (reopened, _sync, stop) = boot_disk_repo(root).await?;
        let big_repo::DocLookup::Ready(handle) = reopened.get_doc(&document).await? else {
            panic!("early-error teardown lost the durable document");
        };
        let retained = handle
            .with_document_read(|doc| {
                doc.get(automerge::ROOT, "note")
                    .unwrap()
                    .unwrap()
                    .0
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .await;
        assert_eq!(retained, "retained after refusal");
        drop(handle);
        stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn memory_context_early_error_drains_native_actor_before_removing_files() -> Res<()> {
        let mut witness = None;
        let failed: Res<()> = async {
            let context = test_cx(utils_rs::function_full!()).await?;
            let document = context.rt.rcx.doc_config.document_id();
            assert!(matches!(
                context._acx.get_doc(&document).await?,
                big_repo::DocLookup::Ready(_)
            ));
            witness = Some((
                Arc::clone(&context._acx),
                document,
                context._temp_dir.path().to_owned(),
            ));
            eyre::bail!("scenario refused after native bootstrap");
        }
        .await;
        let failure = failed.expect_err("exercise early-error teardown");
        let (repo, document, directory) = witness.ok_or(failure)?;
        assert!(
            repo.get_doc(&document).await.is_err(),
            "the native actor must not serve after its context leaves scope"
        );
        assert!(
            !directory.exists(),
            "temporary files remain until teardown completes"
        );
        Ok(())
    }
}
