use crate::interlude::*;

use crate::test::WflowTestContext;
use crate::{AtomicKvSnapStore, KvStore, KvStoreLog, KvStoreMetadtaStore, SqliteKvStore};

#[tokio::test(flavor = "multi_thread")]
async fn test_fails_once() -> Res<()> {
    utils_rs::testing::setup_tracing().unwrap();

    let test_cx = WflowTestContext::new().await?;

    // Register the test_wflows workload
    test_cx
        .register_workload(
            "../../target/wasm32-wasip2/debug/test_wflows.wasm",
            vec!["fails_once".to_string()],
        )
        .await?;

    // Schedule the job - it should fail the first time
    let job_id: Arc<str> = "test-fails-once-1".into();
    let args_json = serde_json::to_string(&serde_json::json!({
        "key": "test-counter"
    }))?;

    test_cx
        .schedule_job(job_id.clone(), "fails_once", args_json.clone())
        .await?;

    // Wait until there are no active jobs (job completed or archived)
    test_cx.wait_until_no_active_jobs(10).await?;

    tracing::info!("wait_until_no_active_jobs completed, getting snapshot");

    // Snapshot the full partition log
    test_cx
        .assert_partition_log_snapshot("fails_once_partition_log")
        .await?;

    // Cleanup
    test_cx.close().await?;
    tracing::info!("test complete");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fails_once_sqlite() -> Res<()> {
    utils_rs::testing::setup_tracing().unwrap();

    // Create an in-memory SQLite database
    use std::str::FromStr;
    let db_pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::from_str("sqlite::memory:")?.create_if_missing(true),
    )
    .await
    .wrap_err("failed to create in-memory SQLite database")?;

    // Create separate SQLite stores for each component
    // Wrap in Arc<Arc<>> to match the pattern used by in-memory stores
    // This is needed because AtomicKvSnapStore::new requires a concrete type S where S: KvStore
    // and Arc<SqliteKvStore> implements KvStore, so Arc<Arc<SqliteKvStore>> works
    let metastore_kv = Arc::new(Arc::new(
        SqliteKvStore::new(db_pool.clone(), "test_metastore").await?,
    ));
    let log_store_kv = Arc::new(Arc::new(
        SqliteKvStore::new(db_pool.clone(), "test_log_store").await?,
    ));
    let snapstore_kv = Arc::new(Arc::new(
        SqliteKvStore::new(db_pool.clone(), "test_snapstore").await?,
    ));

    // Create the stores
    let metastore = Arc::new(
        KvStoreMetadtaStore::new(
            metastore_kv.clone() as Arc<dyn KvStore + Send + Sync>,
            wflow_core::gen::metastore::PartitionsMeta {
                version: "0".into(),
                partition_count: 1,
            },
        )
        .await?,
    );

    let log_store = Arc::new(KvStoreLog::new(
        log_store_kv.clone() as Arc<dyn KvStore + Send + Sync>,
        0,
    ));
    let snapstore = Arc::new(AtomicKvSnapStore::new(snapstore_kv));

    // Build test context with SQLite stores
    let test_cx = WflowTestContext::builder()
        .with_metastore(metastore)
        .with_log_store(log_store)
        .with_snapstore(snapstore)
        .build()
        .await?
        .start()
        .await?;

    // Register the test_wflows workload
    test_cx
        .register_workload(
            "../../target/wasm32-wasip2/debug/test_wflows.wasm",
            vec!["fails_once".to_string()],
        )
        .await?;

    // Schedule the job - it should fail the first time
    let job_id: Arc<str> = "test-fails-once-sqlite-1".into();
    let args_json = serde_json::to_string(&serde_json::json!({
        "key": "test-counter"
    }))?;

    test_cx
        .schedule_job(job_id.clone(), "fails_once", args_json.clone())
        .await?;

    // Wait until there are no active jobs (job completed or archived)
    test_cx.wait_until_no_active_jobs(10).await?;

    tracing::info!("wait_until_no_active_jobs completed, getting snapshot");

    // Snapshot the full partition log
    test_cx
        .assert_partition_log_snapshot("fails_once_sqlite_partition_log")
        .await?;

    // Cleanup
    test_cx.close().await?;
    tracing::info!("test complete");

    Ok(())
}
