use crate::interlude::*;

use utils_rs::am::AmCtx;
use wflow::test::WflowTestContext;

use crate::drawer::DrawerRepo;
use crate::triage::DocTriageWorkerHandle;

mod doc_created_wflow;

pub struct DaybookTestContext {
    pub am_ctx: Arc<AmCtx>,
    pub drawer_repo: Arc<DrawerRepo>,
    pub wflow_test_cx: WflowTestContext,
    _doc_changes_worker: DocTriageWorkerHandle,
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
    let acx = Arc::new(acx);

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

    // Load the drawer repo (DrawerRepo::load takes ownership of AmCtx, so we clone)
    let drawer_repo = DrawerRepo::load((*acx).clone(), drawer_doc_id).await?;

    // Load the config repo
    let config_repo = crate::config::ConfigRepo::load((*acx).clone(), app_doc_id).await?;

    // Initialize default pseudo-labeler processor if needed
    let triage_config = config_repo.get_triage_config_sync().await;

    if triage_config.processors.is_empty() {
        use crate::gen::doc::{DocContentKind, DocTagKind};
        use crate::triage::predicates::PredicateClause;
        use crate::triage::{CancellationPolicy, Processor};

        let predicate = PredicateClause::And(vec![
            PredicateClause::IsContentKind(DocContentKind::Text),
            PredicateClause::Not(Box::new(PredicateClause::HasTag(DocTagKind::PseudoLabel))),
        ]);
        let predicate_json = serde_json::to_value(&predicate).expect("error serializing predicate");
        let processor = Processor {
            cancellation_policy: CancellationPolicy::NoSupport,
            predicate: utils_rs::am::AutosurgeonJson(predicate_json),
            wflow_key: "pseudo-labeler".to_string(),
        };
        config_repo
            .add_processor("pseudo-labeler".to_string(), processor)
            .await?;
    }

    let daybook_plugin = Arc::new(crate::wash_plugin::DaybookPlugin::new(drawer_repo.clone()));
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
            vec!["pseudo-labeler".to_string()],
        )
        .await?;

    // Start the DocTriageWorker to automatically queue jobs when docs are added
    let doc_changes_worker = crate::triage::spawn_doc_triage_worker(
        drawer_repo.clone(),
        wflow_test_cx.ingress.clone(),
        config_repo,
    )
    .await?;

    Ok(DaybookTestContext {
        am_ctx: acx,
        drawer_repo,
        wflow_test_cx,
        _doc_changes_worker: doc_changes_worker,
    })
}
