use crate::interlude::*;

use utils_rs::am::AmCtx;

use crate::drawer::DrawerRepo;
use crate::plugs::PlugsRepo;
use crate::rt::triage::DocTriageWorkerHandle;

mod doc_created_wflow;

pub struct DaybookTestContext {
    pub _acx: AmCtx,
    pub drawer_repo: Arc<DrawerRepo>,
    pub wflow_test_cx: DaybookWflowTestContext,
    _doc_changes_worker: DocTriageWorkerHandle,
    pub _dispatch_repo: Arc<crate::rt::dispatch::DispatchRepo>,
    pub drawer_stop: crate::repos::RepoStopToken,
    pub plugs_stop: crate::repos::RepoStopToken,
    pub config_stop: crate::repos::RepoStopToken,
    pub dispatch_stop: crate::repos::RepoStopToken,
    pub acx_stop: utils_rs::am::AmCtxStopToken,
    pub rt_stop: crate::rt::RtStopToken,
}

impl DaybookTestContext {
    pub async fn stop(self) -> Res<()> {
        self.wflow_test_cx.stop().await?;
        self.drawer_stop.stop().await?;
        self.plugs_stop.stop().await?;
        self.config_stop.stop().await?;
        self.dispatch_stop.stop().await?;
        self.acx_stop.stop().await?;
        self.rt_stop.stop().await?;
        Ok(())
    }
}

pub async fn test_cx(_test_name: &'static str) -> Res<DaybookTestContext> {
    use automerge::transaction::Transactable;
    tokio::task::block_in_place(|| {
        utils_rs::testing::load_envs_once();
        utils_rs::testing::setup_tracing().ok();
    });

    // Initialize AmCtx with memory storage
    let (acx, acx_stop) = AmCtx::boot(
        utils_rs::am::Config {
            peer_id: "test".to_string(),
            storage: utils_rs::am::StorageConfig::Memory,
        },
        Option::<samod::AlwaysAnnounce>::None,
    )
    .await?;

    // Create a drawer document
    let drawer_doc_id = {
        let doc = automerge::Automerge::load(&crate::drawer::version_updates::version_latest()?)?;
        let handle = acx.add_doc(doc).await?;
        handle.document_id().clone()
    };

    // Create an app document for all stores (config, plugs, dispatch, triage)
    let app_doc_id = {
        let doc = automerge::Automerge::load(&crate::app::version_updates::version_latest()?)?;
        let handle = acx.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let temp_dir = tempfile::tempdir()?;
    let blobs = crate::blobs::BlobsRepo::new(temp_dir.path().join("blobs")).await?;

    let (drawer_repo, drawer_stop) = DrawerRepo::load(acx.clone(), drawer_doc_id).await?;
    let (plug_repo, plugs_stop) =
        PlugsRepo::load(acx.clone(), blobs.clone(), app_doc_id.clone()).await?;
    let (_config_repo, config_stop) =
        crate::config::ConfigRepo::load(acx.clone(), app_doc_id.clone(), plug_repo.clone()).await?;
    let (dispatch_repo, dispatch_stop) =
        crate::rt::dispatch::DispatchRepo::load(acx.clone(), app_doc_id.clone()).await?;

    plug_repo.ensure_system_plugs().await?;

    let db_path = temp_dir.path().join("wflow.db");
    let db_pool =
        sqlx::SqlitePool::connect(&format!("sqlite:{}?mode=rwc", db_path.display())).await?;
    sqlx::query("PRAGMA journal_mode=WAL")
        .execute(&db_pool)
        .await?;
    let wcx = wflow::Ctx::init(&db_pool).await?;
    let (rt, rt_stop) = crate::rt::Rt::boot(
        wcx,
        acx.clone(),
        drawer_repo.clone(),
        plug_repo.clone(),
        dispatch_repo.clone(),
        blobs.clone(),
    )
    .await?;

    // Start the DocTriageWorker to automatically queue jobs when docs are added
    let doc_changes_worker =
        crate::rt::triage::spawn_doc_triage_worker(rt.clone(), app_doc_id).await?;

    Ok(DaybookTestContext {
        _acx: acx,
        drawer_repo,
        _dispatch_repo: dispatch_repo,
        wflow_test_cx: DaybookWflowTestContext {
            _wcx: rt.wcx.clone(),
            _wash_host: rt.wash_host.clone(),
            _ingress: rt.wflow_ingress.clone(),
        },
        _doc_changes_worker: doc_changes_worker,
        drawer_stop,
        plugs_stop,
        config_stop,
        dispatch_stop,
        acx_stop,
        rt_stop,
    })
}

pub struct DaybookWflowTestContext {
    pub _wcx: wflow::Ctx,
    pub _wash_host: Arc<wash_runtime::host::Host>,
    pub _ingress: Arc<dyn wflow::WflowIngress>,
}

impl DaybookWflowTestContext {
    pub async fn stop(self) -> Res<()> {
        Ok(())
    }

    pub async fn wait_until_no_active_jobs(&self, _timeout_secs: u64) -> Res<()> {
        // FIXME: implement this properly if needed
        Ok(())
    }
}
