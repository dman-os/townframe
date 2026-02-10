use crate::interlude::*;
use automerge::transaction::Transactable;

use utils_rs::am::AmCtx;

use crate::drawer::DrawerRepo;
use crate::plugs::PlugsRepo;

mod doc_created_wflow;
mod embed_text_wflow;
mod index_vector_wflow;
mod ocr_image_wflow;

pub struct DaybookTestContext {
    pub _acx: AmCtx,
    pub drawer_repo: Arc<DrawerRepo>,
    pub dispatch_repo: Arc<crate::rt::dispatch::DispatchRepo>,
    pub _config_repo: Arc<crate::config::ConfigRepo>,
    pub drawer_stop: crate::repos::RepoStopToken,
    pub plugs_stop: crate::repos::RepoStopToken,
    pub config_stop: crate::repos::RepoStopToken,
    pub dispatch_stop: crate::repos::RepoStopToken,
    pub acx_stop: utils_rs::am::AmCtxStopToken,
    pub rt_stop: crate::rt::RtStopToken,
    pub rt: Arc<crate::rt::Rt>,
    pub _temp_dir: tempfile::TempDir,
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
        self.drawer_stop.stop().await?;
        self.plugs_stop.stop().await?;
        self.config_stop.stop().await?;
        self.dispatch_stop.stop().await?;
        self.acx_stop.stop().await?;
        Ok(())
    }
}

pub async fn test_cx(_test_name: &'static str) -> Res<DaybookTestContext> {
    tokio::task::block_in_place(|| {
        utils_rs::testing::load_envs_once();
        utils_rs::testing::setup_tracing_once();
    });

    // Generate unique IDs for this test to ensure complete isolation across parallel test runs
    let device_id = format!("test_{}", uuid::Uuid::new_v4().simple());
    let peer_id = format!("test_{}", uuid::Uuid::new_v4().simple());

    // Initialize AmCtx with memory storage
    let (acx, acx_stop) = AmCtx::boot(
        utils_rs::am::Config {
            peer_id,
            storage: utils_rs::am::StorageConfig::Memory,
        },
        Option::<samod::AlwaysAnnounce>::None,
    )
    .await?;

    // Create a drawer document
    let drawer_doc_id = {
        let mut doc = automerge::Automerge::new();
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "version", "0")?;
        tx.commit();
        let handle = acx.add_doc(doc).await?;
        handle.document_id().clone()
    };

    // Create an app document for all stores (config, plugs, dispatch, triage)
    let app_doc_id = {
        let doc = automerge::Automerge::load(&crate::app::version_updates::version_latest()?)?;
        let handle = acx.add_doc(doc).await?;
        handle.document_id().clone()
    };

    // Load config first to get local identity
    let local_user_path = daybook_types::doc::UserPath::from("/test-user");
    let local_actor_id = daybook_types::doc::user_path::to_actor_id(&local_user_path);

    let temp_dir = tempfile::tempdir()?;
    let blobs = crate::blobs::BlobsRepo::new(temp_dir.path().join("blobs")).await?;

    let (drawer_repo, drawer_stop) = DrawerRepo::load(
        acx.clone(),
        drawer_doc_id,
        local_actor_id.clone(),
        Arc::new(std::sync::Mutex::new(
            crate::drawer::lru::KeyedLruPool::new(1000),
        )),
        Arc::new(std::sync::Mutex::new(
            crate::drawer::lru::KeyedLruPool::new(1000),
        )),
    )
    .await?;
    let (plug_repo, plugs_stop) = PlugsRepo::load(
        acx.clone(),
        Arc::clone(&blobs),
        app_doc_id.clone(),
        local_actor_id.clone(),
    )
    .await?;
    let (config_repo, config_stop) = crate::config::ConfigRepo::load(
        acx.clone(),
        app_doc_id.clone(),
        Arc::clone(&plug_repo),
        local_user_path.clone(),
    )
    .await?;
    let (dispatch_repo, dispatch_stop) = crate::rt::dispatch::DispatchRepo::load(
        acx.clone(),
        app_doc_id.clone(),
        local_actor_id.clone(),
    )
    .await?;

    plug_repo.ensure_system_plugs().await?;

    let db_path = temp_dir.path().join("wflow.db");
    let wflow_db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let (rt, rt_stop) = crate::rt::Rt::boot(
        crate::rt::RtConfig {
            device_id: device_id.clone(),
        },
        app_doc_id,
        wflow_db_url,
        acx.clone(),
        Arc::clone(&drawer_repo),
        Arc::clone(&plug_repo),
        Arc::clone(&dispatch_repo),
        Arc::clone(&blobs),
        Arc::clone(&config_repo),
        local_actor_id,
    )
    .await?;

    Ok(DaybookTestContext {
        _acx: acx,
        drawer_repo,
        rt,
        dispatch_repo,
        _config_repo: config_repo,
        drawer_stop,
        plugs_stop,
        config_stop,
        dispatch_stop,
        acx_stop,
        rt_stop,
        _temp_dir: temp_dir,
    })
}
