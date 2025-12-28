use crate::interlude::*;

use utils_rs::am::AmCtx;
use wflow::test::WflowTestContext;

use crate::drawer::DrawerRepo;
use crate::plugs::PlugsRepo;
use crate::rt::triage::DocTriageWorkerHandle;

mod doc_created_wflow;

pub struct DaybookTestContext {
    pub acx: AmCtx,
    pub drawer_repo: Arc<DrawerRepo>,
    pub wflow_test_cx: WflowTestContext,
    _doc_changes_worker: DocTriageWorkerHandle,
    dispatcher_repo: Arc<crate::rt::DispatcherRepo>,
}

impl DaybookTestContext {
    pub async fn close(self) -> Res<()> {
        self.wflow_test_cx.close().await?;
        Ok(())
    }
}

pub async fn test_cx(test_name: &'static str) -> Res<DaybookTestContext> {
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

    let drawer_repo = DrawerRepo::load(acx.clone(), drawer_doc_id).await?;
    let plug_repo = PlugsRepo::load(acx.clone(), app_doc_id.clone()).await?;
    let config_repo =
        crate::config::ConfigRepo::load(acx.clone(), app_doc_id.clone(), plug_repo.clone()).await?;
    let dispatcher_repo = crate::rt::DispatcherRepo::load(acx.clone(), app_doc_id).await?;

    // Initialize default pseudo-label processor if needed
    let triage_config = config_repo.get_triage_config_sync().await;

    if triage_config.processors.is_empty() {
        use crate::rt::triage::PredicateClause;
        use crate::rt::triage::{CancellationPolicy, Processor};
        use daybook_types::doc::DocContentKind;

        let predicate = PredicateClause::And(vec![
            PredicateClause::IsContentKind(DocContentKind::Text),
            PredicateClause::Not(Box::new(PredicateClause::HasKey(
                daybook_types::doc::WellKnownPropTag::PseudoLabel.into(),
            ))),
        ]);
        let predicate_json = serde_json::to_value(&predicate).expect("error serializing predicate");
        let processor = Processor {
            cancellation_policy: CancellationPolicy::NoSupport,
            predicate: utils_rs::am::AutosurgeonJson(predicate_json),
            wflow_key: "pseudo-label".to_string(),
        };
        config_repo
            .add_processor("pseudo-label".to_string(), processor)
            .await?;
    }

    let daybook_plugin = Arc::new(crate::rt::wash_plugin::DaybookPlugin::new(
        drawer_repo.clone(),
        dispatcher_repo.clone(),
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
        acx,
        drawer_repo,
        dispatcher_repo,
        wflow_test_cx,
        _doc_changes_worker: doc_changes_worker,
    })
}
