use crate::interlude::*;

use crate::drawer::DrawerRepo;
use crate::wflows::DocChangesWorker;
use utils_rs::am::AmCtx;
use wflow::test::WflowTestContext;

mod doc_created_wflow;

pub struct DaybookTestContext {
    pub am_ctx: Arc<AmCtx>,
    pub drawer_repo: Arc<DrawerRepo>,
    pub wflow_test_cx: WflowTestContext,
    _doc_changes_worker: DocChangesWorker,
}

impl DaybookTestContext {
    pub async fn close(self) -> Res<()> {
        self.wflow_test_cx.close().await?;
        Ok(())
    }
}

async fn test_cx(test_name: &'static str) -> Res<DaybookTestContext> {
    tokio::task::block_in_place(|| {
        utils_rs::testing::load_envs_once();
    });

    // Initialize AmCtx with memory storage
    let acx = AmCtx::boot(
        utils_rs::am::Config {
            peer_id: "test".to_string(),
            storage: utils_rs::am::StorageConfig::Memory,
        },
        Option::<samod::AlwaysAnnounce>::None,
    )
    .await?;
    let acx = Arc::new(acx);

    // Create a drawer document
    let drawer_doc_id = {
        let doc = automerge::Automerge::load(&crate::drawer::version_updates::version_latest()?)?;
        let handle = acx.add_doc(doc).await?;
        handle.document_id().clone()
    };

    // Load the drawer repo (DrawerRepo::load takes ownership of AmCtx, so we clone)
    let drawer_repo = DrawerRepo::load((*acx).clone(), drawer_doc_id).await?;

    let am_repo_plugin = Arc::new(wash_plugin_am_repo::AmRepoPlugin::new(wcx.acx.clone()));
    let utils_plugin = wash_plugin_utils::UtilsPlugin::new(wash_plugin_utils::Config {
        ollama_url: utils_rs::get_env_var("OLLAMA_URL")?,
        ollama_model: utils_rs::get_env_var("OLLAMA_MODEL")?,
    })
    .wrap_err("error creating utils plugin")?;
    // Create wflow test context with the same AmCtx so documents are shared
    let wflow_test_cx = WflowTestContext::builder()
        .wth_plugin(am_repo_plugin)
        .wth_plugin(utils_plugin)
        .build()
        .await?
        .start()
        .await?;

    // Register the daybook_wflows workload
    wflow_test_cx
        .register_workload(
            "../../target/wasm32-wasip2/debug/daybook_wflows.wasm",
            vec!["doc-created".to_string()],
        )
        .await?;

    // Start the DocChangesWorker to automatically queue jobs when docs are added
    let doc_changes_worker =
        DocChangesWorker::spawn(drawer_repo.clone(), wflow_test_cx.ingress.clone()).await?;

    Ok(DaybookTestContext {
        am_ctx: acx,
        drawer_repo,
        wflow_test_cx,
        _doc_changes_worker: doc_changes_worker,
    })
}
