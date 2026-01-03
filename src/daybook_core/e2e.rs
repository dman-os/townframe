use crate::interlude::*;

use utils_rs::am::AmCtx;
use wflow::test::WflowTestContext;

use crate::drawer::DrawerRepo;
use crate::plugs::PlugsRepo;
use crate::rt::triage::DocTriageWorkerHandle;

mod doc_created_wflow;

pub struct DaybookTestContext {
    pub _acx: AmCtx,
    pub drawer_repo: Arc<DrawerRepo>,
    pub wflow_test_cx: WflowTestContext,
    _doc_changes_worker: DocTriageWorkerHandle,
    pub _dispatch_repo: Arc<crate::rt::dispatch::DispatchRepo>,
    pub drawer_stop: crate::repos::RepoStopToken,
    pub plugs_stop: crate::repos::RepoStopToken,
    pub config_stop: crate::repos::RepoStopToken,
    pub dispatch_stop: crate::repos::RepoStopToken,
    pub acx_stop: utils_rs::am::AmCtxStopToken,
}

impl DaybookTestContext {
    pub async fn stop(self) -> Res<()> {
        self.wflow_test_cx.stop().await?;
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

    // Create an app document for config
    let app_doc_id = {
        let doc = automerge::Automerge::load(&crate::config::version_updates::version_latest()?)?;
        let handle = acx.add_doc(doc).await?;
        handle.document_id().clone()
    };

    let temp_dir = tempfile::tempdir()?;
    let blobs = crate::blobs::BlobsRepo::new(temp_dir.path().join("blobs")).await?;

    let (drawer_repo, drawer_stop) = DrawerRepo::load(acx.clone(), drawer_doc_id).await?;
    let (plug_repo, plugs_stop) = PlugsRepo::load(acx.clone(), blobs, app_doc_id.clone()).await?;
    let (config_repo, config_stop) =
        crate::config::ConfigRepo::load(acx.clone(), app_doc_id.clone(), plug_repo.clone()).await?;
    let (dispatch_repo, dispatch_stop) =
        crate::rt::dispatch::DispatchRepo::load(acx.clone(), app_doc_id).await?;

    // Initialize default pseudo-label processor if needed
    let triage_config = config_repo.get_triage_config_sync().await;

    if triage_config.processors.is_empty() {
        use crate::rt::triage::PredicateClause;
        use crate::rt::triage::{CancellationPolicy, Processor};
        use daybook_types::doc::WellKnownPropTag;

        let predicate = PredicateClause::And(vec![
            PredicateClause::HasKey(WellKnownPropTag::Content.into()),
            PredicateClause::Not(Box::new(PredicateClause::HasKey(
                daybook_types::doc::WellKnownPropTag::PseudoLabel.into(),
            ))),
        ]);
        let processor = Processor {
            cancellation_policy: CancellationPolicy::NoSupport,
            predicate: ThroughJson(predicate),
            wflow_key: "pseudo-label".to_string(),
        };
        config_repo
            .add_processor("pseudo-label".to_string(), processor)
            .await?;
    }

    let daybook_plugin = Arc::new(crate::rt::wash_plugin::DaybookPlugin::new(
        drawer_repo.clone(),
        dispatch_repo.clone(),
    ));
    let utils_plugin = wash_plugin_utils::UtilsPlugin::new(wash_plugin_utils::Config {
        ollama_url: utils_rs::get_env_var("OLLAMA_URL")?,
        ollama_model: utils_rs::get_env_var("OLLAMA_MODEL")?,
    })
    .wrap_err("error creating utils plugin")?;
    // Create wflow test context with the same AmCtx so documents are shared
    let wflow_test_cx = WflowTestContext::builder()
        .with_plugin(daybook_plugin)
        .with_plugin(utils_plugin)
        .build()
        .await?
        .start()
        .await?;

    // Register the daybook_wflows workload
    wflow_test_cx
        .register_workload(
            "../../target/wasm32-wasip2/debug/daybook_wflows.wasm",
            vec!["pseudo-label".to_string()],
        )
        .await?;

    // Start the DocTriageWorker to automatically queue jobs when docs are added
    let doc_changes_worker = crate::rt::triage::spawn_doc_triage_worker(
        drawer_repo.clone(),
        wflow_test_cx.ingress.clone(),
        config_repo,
    )
    .await?;

    Ok(DaybookTestContext {
        _acx: acx,
        drawer_repo,
        _dispatch_repo: dispatch_repo,
        wflow_test_cx,
        _doc_changes_worker: doc_changes_worker,
        drawer_stop,
        plugs_stop,
        config_stop,
        dispatch_stop,
        acx_stop,
    })
}
