use api_utils_rs::prelude::*;
use automerge;
use daybook_core::drawer::DrawerRepo;
use daybook_core::wflows::{
    build_runtime_host, start_partition_worker, DocChangesWorker, PartitionLogIngress,
    RuntimeConfig,
};
use utils_rs::am::AmCtx;
use wash_runtime::{host::HostApi, types, wit::WitInterface};
use wflow::log;
use wflow::metastore;
use wflow::partition::log as partition_log;

#[tokio::test(flavor = "multi_thread")]
async fn test_doc_created() -> Res<()> {
    utils_rs::testing::setup_tracing().unwrap();

    // Initialize AmCtx with memory storage
    let am_ctx = AmCtx::boot(
        utils_rs::am::Config {
            peer_id: "test".to_string(),
            storage: utils_rs::am::StorageConfig::Memory,
        },
        Option::<samod::AlwaysAnnounce>::None,
    )
    .await?;
    let am_ctx = Arc::new(am_ctx);

    // Create drawer document and DrawerRepo
    let drawer_doc_id = {
        let doc =
            automerge::Automerge::load(&daybook_core::drawer::version_updates::version_latest()?)?;
        let handle = am_ctx.add_doc(doc).await?;
        handle.document_id().clone()
    };
    let drawer_repo = DrawerRepo::load((*am_ctx).clone(), drawer_doc_id).await?;

    // Create metastore (using same pattern as wflow plugin test)
    use utils_rs::DHashMap;
    let kv = DHashMap::<Arc<[u8]>, Arc<[u8]>>::default();
    let kv = Arc::new(kv);
    let metastore = {
        // The trait is private, but DHashMap implements it, so we can pass it directly
        // The compiler will infer the trait object type internally
        metastore::KvStoreMetadtaStore::new(
            kv,
            metastore::PartitionsMeta {
                version: "0".into(),
                partition_count: 1,
            },
        )
        .await?
    };
    let metastore = Arc::new(metastore);

    // Create log store
    let log_kv = DHashMap::<Arc<[u8]>, Arc<[u8]>>::default();
    let log_kv = Arc::new(log_kv);
    let log_store = log::KvStoreLog::new(log_kv, 0);
    let log_store = Arc::new(log_store);

    // Create partition log reference
    let partition_log = partition_log::PartitionLogRef::new(log_store.clone());

    // Create ingress
    let ingress = PartitionLogIngress::new(partition_log.clone(), metastore.clone());
    let ingress = Arc::new(ingress);

    // Build runtime host
    let config = RuntimeConfig {
        am_ctx: am_ctx.clone(),
        metastore: metastore.clone(),
        log_store: log_store.clone(),
        partition_id: 0,
    };

    let (host, wflow_plugin) = build_runtime_host(config.clone()).await?;

    // Start partition worker
    let worker_handle = start_partition_worker(&config, wflow_plugin).await?;

    // Load daybook_wflows.wasm and register as workload
    let dbook_wflow_wasm =
        tokio::fs::read("../../target/wasm32-wasip2/debug/daybook_wflows.wasm").await?;

    let req = types::WorkloadStartRequest {
        workload: types::Workload {
            namespace: "test".to_string(),
            name: "daybook-wflows".to_string(),
            annotations: std::collections::HashMap::new(),
            service: None,
            components: vec![types::Component {
                bytes: dbook_wflow_wasm.into(),
                ..default()
            }],
            host_interfaces: vec![
                WitInterface {
                    config: [("wflow_keys".to_owned(), "doc-created".to_owned())].into(),
                    ..WitInterface::from("townframe:wflow/bundle")
                },
                WitInterface {
                    ..WitInterface::from("townframe:am-repo/repo")
                },
            ],
            volumes: vec![],
        },
    };

    host.workload_start(req).await.to_eyre()?;

    // Spawn doc changes worker
    let doc_worker = DocChangesWorker::spawn(drawer_repo.clone(), ingress.clone()).await?;

    // Add a document to the drawer
    use daybook_core::gen::doc::{Doc, DocContent};
    let test_doc = Doc {
        id: String::new(), // Will be set by add()
        created_at: time::OffsetDateTime::now_utc(),
        updated_at: time::OffsetDateTime::now_utc(),
        content: DocContent::Text("Test document".to_string()),
        tags: vec![],
    };
    drawer_repo.add(test_doc).await?;

    // Wait for the workflow to complete successfully
    use futures::StreamExt;
    use wflow::log::LogStore;
    use wflow::partition::job_events::{JobEventDeets, JobRunResult};
    use wflow::partition::log::PartitionLogEntry;

    {
        use tokio::time::{sleep, Duration};
        let mut stream = log_store.tail(0).await;
        let mut timeout = sleep(Duration::from_secs(30));
        tokio::pin!(timeout);

        loop {
            let entry = tokio::select! {
                _ = &mut timeout => {
                    return Err(eyre::eyre!("timeout waiting for workflow to complete"));
                }
                entry = stream.next() => {
                    entry
                }
            };
            let Some(Ok((_, entry_bytes))) = entry else {
                continue;
            };

            let log_entry: PartitionLogEntry =
                serde_json::from_slice(&entry_bytes[..]).wrap_err("failed to parse log entry")?;

            match log_entry {
                PartitionLogEntry::JobEvent(job_event) => {
                    match job_event.deets {
                        JobEventDeets::Run(run_event) => {
                            match run_event.result {
                                JobRunResult::Success { .. } => {
                                    info!("Workflow completed successfully!");
                                    break;
                                }
                                JobRunResult::WflowErr(err) => {
                                    return Err(eyre::eyre!("workflow error: {:?}", err));
                                }
                                JobRunResult::WorkerErr(err) => {
                                    return Err(eyre::eyre!("worker error: {:?}", err));
                                }
                                JobRunResult::StepEffect(_) => {
                                    // Still processing, continue waiting
                                }
                            }
                        }
                        JobEventDeets::Init(_) => {
                            // Job initialized, continue waiting
                        }
                    }
                }
                PartitionLogEntry::NewPartitionEffects(_) => {
                    // Effects entry, continue waiting
                }
            }
        }
    }

    info!("XXX done 1");

    // Cleanup: shutdown all workers
    // Abort doc worker first (it may have spawned retry tasks)
    doc_worker.abort();
    // Close partition worker (this will cancel all effect workers and the entry mux)
    worker_handle.close().await?;

    info!("XXX done");

    Ok(())
}
