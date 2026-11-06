use super::tests::active_dispatch;
use super::tests::setup_repo_with_sql;
use super::*;

#[tokio::test]
async fn pending_admission_survives_reload_until_admitted() -> Res<()> {
    let temp = tempfile::tempdir()?;
    let sql_cfg = crate::app::SqlConfig::file(temp.path().join("dispatch.sqlite"));
    let runtime = sqlite_rs::SqliteRuntime::new(default());

    let sql = crate::app::open_database(&runtime, sql_cfg.clone()).await?;
    let (repo, _) = setup_repo_with_sql(sql.clone()).await?;
    let mut pending = (*active_dispatch("job-pending")).clone();
    pending.status = DispatchStatus::AdmissionPending;
    repo.add("disp-pending".into(), Arc::new(pending)).await?;
    drop(repo);
    drop(sql);

    let sql = crate::app::open_database(&runtime, sql_cfg).await?;
    let (repo, _) = setup_repo_with_sql(sql).await?;
    assert_eq!(repo.list_pending_admissions().await.len(), 1);
    assert!(repo.get_active("disp-pending").await.is_none());
    repo.mark_admitted("disp-pending").await?;
    assert!(repo.get_active("disp-pending").await.is_some());
    Ok(())
}

#[tokio::test]
async fn pending_admission_publishes_callback_context_before_mark_admitted() -> Res<()> {
    let temp = tempfile::tempdir()?;
    let sql_cfg = crate::app::SqlConfig::file(temp.path().join("dispatch.sqlite"));
    let runtime = sqlite_rs::SqliteRuntime::new(default());
    let sql = crate::app::open_database(&runtime, sql_cfg).await?;
    let (repo, _) = setup_repo_with_sql(sql).await?;
    let mut pending = (*active_dispatch("job-callback-pending")).clone();
    pending.status = DispatchStatus::AdmissionPending;
    repo.add("disp-callback-pending".into(), Arc::new(pending))
        .await?;

    let callback_dispatch = repo.get_by_wflow_job("job-callback-pending").await;
    assert_eq!(
        callback_dispatch.as_ref().map(|dispatch| &dispatch.status),
        Some(&DispatchStatus::AdmissionPending)
    );
    Ok(())
}

#[tokio::test]
async fn admission_keeps_cancellation_outbox_until_receipt_completion() -> Res<()> {
    let temp = tempfile::tempdir()?;
    let sql_cfg = crate::app::SqlConfig::file(temp.path().join("dispatch.sqlite"));
    let runtime = sqlite_rs::SqliteRuntime::new(default());
    let sql = crate::app::open_database(&runtime, sql_cfg).await?;
    let (repo, _) = setup_repo_with_sql(sql).await?;
    let mut pending = (*active_dispatch("job-cancel-before-submit")).clone();
    pending.status = DispatchStatus::AdmissionPending;
    repo.add("disp-cancel-before-submit".into(), Arc::new(pending))
        .await?;

    assert!(repo.mark_cancelled("disp-cancel-before-submit").await?);
    let admitted = repo.mark_admitted("disp-cancel-before-submit").await?;
    assert_eq!(admitted.status, DispatchStatus::Active);
    assert!(repo.is_cancelled("disp-cancel-before-submit").await);
    // Admission acknowledgement is not cancellation completion. Until the
    // authority receipt is handled, boot must retain both context and intent.
    assert!(repo.get_active("disp-cancel-before-submit").await.is_some());
    assert!(
        repo.get_by_wflow_job("job-cancel-before-submit")
            .await
            .is_some()
    );
    repo.complete(
        "disp-cancel-before-submit".into(),
        DispatchStatus::Cancelled,
    )
    .await?;
    assert!(!repo.is_cancelled("disp-cancel-before-submit").await);
    assert!(repo.get_active("disp-cancel-before-submit").await.is_none());
    assert_eq!(
        repo.mark_admitted("disp-cancel-before-submit")
            .await?
            .status,
        DispatchStatus::Cancelled
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_before_submission_terminalizes_waiting_intent() -> Res<()> {
    let temp = tempfile::tempdir()?;
    let sql_cfg = crate::app::SqlConfig::file(temp.path().join("dispatch.sqlite"));
    let runtime = sqlite_rs::SqliteRuntime::new(default());
    let sql = crate::app::open_database(&runtime, sql_cfg).await?;
    let (repo, _) = setup_repo_with_sql(sql).await?;
    let mut waiting = (*active_dispatch("job-cancel-waiting")).clone();
    waiting.status = DispatchStatus::Waiting;
    repo.add("disp-cancel-waiting".into(), Arc::new(waiting))
        .await?;

    repo.complete("disp-cancel-waiting".into(), DispatchStatus::Cancelled)
        .await?;
    assert_eq!(
        repo.get_any("disp-cancel-waiting")
            .await
            .expect("cancelled waiting intent")
            .status,
        DispatchStatus::Cancelled
    );
    assert!(repo.get_by_wflow_job("job-cancel-waiting").await.is_none());
    Ok(())
}

#[tokio::test]
async fn cancellation_outbox_survives_reload_before_and_after_admission() -> Res<()> {
    let temp = tempfile::tempdir()?;
    let sql_cfg = crate::app::SqlConfig::file(temp.path().join("dispatch.sqlite"));
    let runtime = sqlite_rs::SqliteRuntime::new(default());

    let sql = crate::app::open_database(&runtime, sql_cfg.clone()).await?;
    let (repo, _) = setup_repo_with_sql(sql).await?;
    let mut pending = (*active_dispatch("job-cancelled-reload")).clone();
    pending.status = DispatchStatus::AdmissionPending;
    repo.add("disp-cancelled-reload".into(), Arc::new(pending))
        .await?;
    assert!(repo.mark_cancelled("disp-cancelled-reload").await?);
    drop(repo);

    let sql = crate::app::open_database(&runtime, sql_cfg.clone()).await?;
    let (repo, _) = setup_repo_with_sql(sql).await?;
    assert!(repo.is_cancelled("disp-cancelled-reload").await);
    assert_eq!(repo.list_pending_admissions().await.len(), 1);
    let reconciled = repo.mark_admitted("disp-cancelled-reload").await?;
    assert_eq!(reconciled.status, DispatchStatus::Active);
    assert!(repo.is_cancelled("disp-cancelled-reload").await);
    drop(repo);

    // Simulate a crash after local admission but before cancellation receipt
    // post-processing. The cancellation intent still participates in boot.
    let sql = crate::app::open_database(&runtime, sql_cfg).await?;
    let (repo, _) = setup_repo_with_sql(sql).await?;
    assert!(repo.is_cancelled("disp-cancelled-reload").await);
    assert_eq!(repo.list_inflight().await.len(), 1);
    assert!(repo.get_active("disp-cancelled-reload").await.is_some());
    repo.complete("disp-cancelled-reload".into(), DispatchStatus::Cancelled)
        .await?;
    assert!(!repo.is_cancelled("disp-cancelled-reload").await);
    assert!(repo.list_inflight().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn concurrent_admission_reconcilers_do_not_stale_transition() -> Res<()> {
    let temp = tempfile::tempdir()?;
    let sql_cfg = crate::app::SqlConfig::file(temp.path().join("dispatch.sqlite"));
    let runtime = sqlite_rs::SqliteRuntime::new(default());
    let sql = crate::app::open_database(&runtime, sql_cfg).await?;
    let (repo, _) = setup_repo_with_sql(sql).await?;
    let mut pending = (*active_dispatch("job-concurrent-admission")).clone();
    pending.status = DispatchStatus::AdmissionPending;
    repo.add("disp-concurrent-admission".into(), Arc::new(pending))
        .await?;

    let (first, second) = tokio::join!(
        repo.mark_admitted("disp-concurrent-admission"),
        repo.mark_admitted("disp-concurrent-admission"),
    );
    assert_eq!(first?.status, DispatchStatus::Active);
    assert_eq!(second?.status, DispatchStatus::Active);
    assert!(repo.get_active("disp-concurrent-admission").await.is_some());
    Ok(())
}
