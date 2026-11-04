use crate::interlude::*;

use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use sqlite_rs::RuntimeConfig;
use sqlite_rs::SqliteRuntime;
use wflow_core::partition::authority::Disposition;
use wflow_tokio::authority_runtime::AuthorityRuntime;
use wflow_tokio::authority_runtime::ExpireError;
use wflow_tokio::authority_runtime::Submission;
use wflow_tokio::local_native_host::LocalNativeHost;
use wflow_wdk::Json;

use crate::kvstore::atomic::SqliteFactory;

const NAMESPACE: &str = "authority";
const PARTITION_INCARNATION: u64 = 1;

async fn start(path: &Path, host: &LocalNativeHost) -> Res<(SqliteFactory, AuthorityRuntime)> {
    let database = SqliteRuntime::new(RuntimeConfig::default()).database(path, &[], "_unused");
    let factory = SqliteFactory::boot(database).await?;
    let runtime = AuthorityRuntime::start(
        factory.writer(NAMESPACE).await?,
        PARTITION_INCARNATION,
        Arc::new(host.clone()),
    )
    .await?;
    Ok((factory, runtime))
}

/// Registers `steps`, which runs `n` effects returning their index and sums
/// them; every effect closure that actually runs bumps `runs`.
fn steps_host(runs: &Arc<AtomicU64>) -> LocalNativeHost {
    let host = LocalNativeHost::new();
    let runs = Arc::clone(runs);
    host.register_typed("steps", move |cx, n: u64| {
        let runs = Arc::clone(&runs);
        Box::pin(async move {
            let mut sum = 0;
            for index in 0..n {
                let runs = Arc::clone(&runs);
                sum += cx
                    .effect(|| async move {
                        runs.fetch_add(1, Ordering::SeqCst);
                        Ok(Json(index))
                    })
                    .await?;
            }
            Ok(sum)
        })
    });
    host
}

#[tokio::test]
async fn sqlite_completed_job_survives_restart_without_rerunning_effects() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("wflow.sqlite");
    let expected: wflow_wdk::types::JobResult = Ok(serde_json::to_string(&3_u64)?);

    let runs = Arc::new(AtomicU64::new(0));
    let host = steps_host(&runs);
    let (factory, runtime) = start(&path, &host).await?;
    let admitted = runtime
        .submit(Submission::new("job", "steps", "3"))
        .await
        .expect("steps is registered and job is fresh");
    assert_eq!(admitted.disposition, Disposition::Admitted);
    assert_eq!(runtime.await_result("job").await, Some(expected.clone()));
    assert_eq!(runs.load(Ordering::SeqCst), 3);
    assert_eq!(runtime.result("job").await?, Some(expected.clone()));
    runtime.stop().await;
    drop(factory);
    drop(host);

    let runs = Arc::new(AtomicU64::new(0));
    let host = steps_host(&runs);
    let (_factory, runtime) = start(&path, &host).await?;
    assert_eq!(runtime.result("job").await?, Some(expected.clone()));
    let resubmitted = runtime
        .submit(Submission::new("job", "steps", "3"))
        .await
        .expect("resubmitting the same invocation is accepted");
    assert_eq!(resubmitted.position, admitted.position);
    assert_eq!(resubmitted.disposition, Disposition::Completed);
    assert_eq!(runtime.await_result("job").await, Some(expected));
    assert_eq!(runs.load(Ordering::SeqCst), 0);

    // Compact over SQLite, expire a zero-retention job, reuse its key under a
    // fresh incarnation, compact again, and verify across restart.
    let (_, short_handle) = runtime
        .submit_handle(Submission::new("short", "steps", "2").retention_ms(0))
        .await
        .expect("short job admitted");
    let expected_short: wflow_wdk::types::JobResult = Ok(serde_json::to_string(&1_u64)?);
    assert_eq!(short_handle.await_result().await, Some(expected_short));
    assert!(matches!(
        runtime.expire("short").await,
        Ok(_) | Err(ExpireError::UnknownJob)
    ));
    while runtime.receipt("short").await?.is_some() {
        tokio::task::yield_now().await;
    }
    assert_eq!(runtime.get_result("short").await?, None);
    assert_eq!(runtime.expire("short").await, Err(ExpireError::UnknownJob));

    let (reused_ack, reused_handle) = runtime
        .submit_handle(Submission::new("short", "steps", "4").retention_ms(60_000))
        .await
        .expect("expired key can be reused with a new invocation");
    assert_eq!(reused_ack.disposition, Disposition::Admitted);
    let expected_reused: wflow_wdk::types::JobResult = Ok(serde_json::to_string(&6_u64)?);
    assert_eq!(
        reused_handle.await_result().await,
        Some(expected_reused.clone())
    );
    let reused_receipt = reused_handle.receipt().await?.expect("reused receipt");
    assert_eq!(reused_receipt.incarnation, 3);
    assert_eq!(
        runtime.expire("short").await,
        Err(ExpireError::RetentionPending {
            expires_at_ms: reused_receipt.outcome.expires_at_ms(),
        })
    );

    let cut = runtime.compact().await?;
    assert!(cut > 0);
    runtime.stop().await;
    drop(_factory);
    drop(host);

    let runs = Arc::new(AtomicU64::new(0));
    let host = steps_host(&runs);
    let (_factory, runtime) = start(&path, &host).await?;
    assert_eq!(runtime.get_result("short").await?, Some(expected_reused));
    assert_eq!(
        runtime
            .receipt("short")
            .await?
            .expect("recovered receipt")
            .incarnation,
        3
    );
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    runtime.stop().await;
    Ok(())
}
