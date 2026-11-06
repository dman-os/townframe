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
