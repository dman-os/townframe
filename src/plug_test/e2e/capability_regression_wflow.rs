use daybook_types::doc::{AddDocArgs, FacetKey, FacetRaw, WellKnownFacet, WellKnownFacetTag};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqlitePool};
use std::str::FromStr;
use api_utils_rs::prelude::*;

async fn open_plug_test_local_state(
    test_cx: &daybook_core::test_support::DaybookTestContext,
) -> Res<SqlitePool> {
    let sqlite_file_path = test_cx
        .rt
        .sqlite_local_state_repo
        .get_sqlite_file_path("@daybook/test/capability-report")
        .await?;
    let db_url = format!("sqlite:{}?mode=rwc", sqlite_file_path.display());
    let connect_options = SqliteConnectOptions::from_str(&db_url)?
        .create_if_missing(true)
        .disable_statement_logging();
    Ok(SqlitePool::connect_with(connect_options).await?)
}

async fn dispatch_and_wait(
    test_cx: &daybook_core::test_support::DaybookTestContext,
    routine_name: &str,
    doc_id: &String,
    changed_facet_keys: Vec<String>,
) -> Res<String> {
    let (_doc, heads) = test_cx
        .drawer_repo
        .get_with_heads(
            doc_id,
            &daybook_types::doc::BranchPath::from("main"),
            None,
        )
        .await?
        .ok_or_eyre("doc not found")?;

    let dispatch_id = test_cx
        .rt
        .dispatch(
            "@daybook/test",
            routine_name,
            daybook_core::rt::DispatchArgs::DocRoutine {
                doc_id: doc_id.clone(),
                branch_path: daybook_types::doc::BranchPath::from("main"),
                heads,
                changed_facet_keys,
                wflow_args_json: None,
            },
        )
        .await?;

    test_cx
        .rt
        .wait_for_dispatch_end(&dispatch_id, std::time::Duration::from_secs(120))
        .await?;

    let dispatch = test_cx
        .dispatch_repo
        .get_any(&dispatch_id)
        .await
        .ok_or_eyre("missing dispatch after completion")?;

    assert!(
        matches!(
            dispatch.status,
            daybook_core::rt::dispatch::DispatchStatus::Succeeded
        ),
        "dispatch {dispatch_id} for {routine_name} did not succeed: {:?}",
        dispatch.status
    );

    Ok(dispatch_id)
}

async fn fetch_capability_report(
    db_pool: &SqlitePool,
    doc_id: &str,
) -> Res<serde_json::Value> {
    let summary_json: String = sqlx::query_scalar(
        "SELECT summary_json FROM capability_report WHERE doc_id = ?1",
    )
    .bind(doc_id)
    .fetch_one(db_pool)
    .await
    .wrap_err_with(|| format!("no capability_report row for doc_id={doc_id}"))?;

    Ok(serde_json::from_str(&summary_json)?)
}

async fn setup_doc(test_cx: &daybook_core::test_support::DaybookTestContext) -> Res<String> {
    let doc_id = test_cx
        .drawer_repo
        .add(AddDocArgs {
            branch_path: daybook_types::doc::BranchPath::from("main"),
            facets: [
                (
                    FacetKey::from(WellKnownFacetTag::LabelGeneric),
                    FacetRaw::from(WellKnownFacet::LabelGeneric("seed".into())),
                ),
                (
                    FacetKey::from(WellKnownFacetTag::Note),
                    FacetRaw::from(WellKnownFacet::Note(daybook_types::doc::Note {
                        mime: "text/plain".into(),
                        content: "test note".into(),
                    })),
                ),
            ]
            .into(),
            user_path: None,
        })
        .await?;
    Ok(doc_id)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_full_command_capability_report() -> Res<()> {
    let test_cx = daybook_core::test_support::test_cx("cap_reg_full_cmd").await?;
    super::common::import_test_plug_oci(&test_cx).await?;

    let doc_id = setup_doc(&test_cx).await?;
    dispatch_and_wait(&test_cx, "report-full-command", &doc_id, vec![]).await?;

    let db_pool = open_plug_test_local_state(&test_cx).await?;
    let report = fetch_capability_report(&db_pool, &doc_id).await?;

    assert_eq!(report["invocation"]["kind"], "Command");

    let rw_keys: Vec<String> = serde_json::from_value(report["rw_facet_keys"].clone())?;
    assert!(rw_keys.contains(&"org.example.daybook.labelgeneric/main".to_string()));

    let ro_keys: Vec<String> = serde_json::from_value(report["ro_facet_keys"].clone())?;
    assert!(ro_keys.contains(&"org.example.daybook.note/main".to_string()));

    let rw_config: Vec<String> =
        serde_json::from_value(report["rw_config_facet_keys"].clone())?;
    assert!(rw_config.contains(&"org.example.test.config/main".to_string()));

    let ro_config: Vec<String> =
        serde_json::from_value(report["ro_config_facet_keys"].clone())?;
    assert!(ro_config.contains(&"org.example.test.config-ro/main".to_string()));

    let cmd_urls: Vec<String> = serde_json::from_value(report["command_invoke_urls"].clone())?;
    assert!(!cmd_urls.is_empty());

    let sqlite_conns: Vec<String> =
        serde_json::from_value(report["sqlite_connections"].clone())?;
    assert!(sqlite_conns.contains(&"@daybook/test/capability-report".to_string()));

    test_cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_full_processor_capability_report() -> Res<()> {
    let test_cx = daybook_core::test_support::test_cx("cap_reg_full_proc").await?;
    super::common::import_test_plug_oci(&test_cx).await?;

    let doc_id = setup_doc(&test_cx).await?;
    let changed_key = "org.example.daybook.note/main".to_string();
    dispatch_and_wait(
        &test_cx,
        "report-full-processor",
        &doc_id,
        vec![changed_key.clone()],
    )
    .await?;

    let db_pool = open_plug_test_local_state(&test_cx).await?;
    let report = fetch_capability_report(&db_pool, &doc_id).await?;

    assert_eq!(report["invocation"]["kind"], "Processor");
    assert_eq!(report["invocation"]["trigger_doc_id"], doc_id);

    let changed: Vec<String> =
        serde_json::from_value(report["invocation"]["changed_facet_keys"].clone())?;
    assert!(changed.contains(&changed_key));

    let rw_keys: Vec<String> = serde_json::from_value(report["rw_facet_keys"].clone())?;
    assert!(rw_keys.contains(&"org.example.daybook.labelgeneric/main".to_string()));

    let ro_keys: Vec<String> = serde_json::from_value(report["ro_facet_keys"].clone())?;
    assert!(ro_keys.contains(&"org.example.daybook.note/main".to_string()));

    let rw_config: Vec<String> =
        serde_json::from_value(report["rw_config_facet_keys"].clone())?;
    assert!(rw_config.contains(&"org.example.test.config/main".to_string()));

    let ro_config: Vec<String> =
        serde_json::from_value(report["ro_config_facet_keys"].clone())?;
    assert!(ro_config.contains(&"org.example.test.config-ro/main".to_string()));

    let cmd_urls: Vec<String> = serde_json::from_value(report["command_invoke_urls"].clone())?;
    assert!(cmd_urls.is_empty(), "processor should not have command invoke tokens");

    let sqlite_conns: Vec<String> =
        serde_json::from_value(report["sqlite_connections"].clone())?;
    assert!(sqlite_conns.contains(&"@daybook/test/capability-report".to_string()));

    test_cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_minimal_command_capability_report() -> Res<()> {
    let test_cx = daybook_core::test_support::test_cx("cap_reg_min_cmd").await?;
    super::common::import_test_plug_oci(&test_cx).await?;

    let doc_id = setup_doc(&test_cx).await?;
    dispatch_and_wait(&test_cx, "report-minimal-command", &doc_id, vec![]).await?;

    let db_pool = open_plug_test_local_state(&test_cx).await?;
    let report = fetch_capability_report(&db_pool, &doc_id).await?;

    assert_eq!(report["invocation"]["kind"], "Command");

    let rw_keys: Vec<String> = serde_json::from_value(report["rw_facet_keys"].clone())?;
    assert!(rw_keys.contains(&"org.example.daybook.labelgeneric/main".to_string()));

    let ro_keys: Vec<String> = serde_json::from_value(report["ro_facet_keys"].clone())?;
    assert!(ro_keys.is_empty(), "minimal should have no ro facet keys");

    let rw_config: Vec<String> =
        serde_json::from_value(report["rw_config_facet_keys"].clone())?;
    assert!(rw_config.is_empty(), "minimal should have no rw config keys");

    let ro_config: Vec<String> =
        serde_json::from_value(report["ro_config_facet_keys"].clone())?;
    assert!(ro_config.is_empty(), "minimal should have no ro config keys");

    let cmd_urls: Vec<String> = serde_json::from_value(report["command_invoke_urls"].clone())?;
    assert!(cmd_urls.is_empty(), "minimal should have no command invoke tokens");

    let sqlite_conns: Vec<String> =
        serde_json::from_value(report["sqlite_connections"].clone())?;
    assert!(sqlite_conns.contains(&"@daybook/test/capability-report".to_string()));

    test_cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_minimal_processor_capability_report() -> Res<()> {
    let test_cx = daybook_core::test_support::test_cx("cap_reg_min_proc").await?;
    super::common::import_test_plug_oci(&test_cx).await?;

    let doc_id = setup_doc(&test_cx).await?;
    let changed_key = "org.example.daybook.note/main".to_string();
    dispatch_and_wait(
        &test_cx,
        "report-minimal-processor",
        &doc_id,
        vec![changed_key.clone()],
    )
    .await?;

    let db_pool = open_plug_test_local_state(&test_cx).await?;
    let report = fetch_capability_report(&db_pool, &doc_id).await?;

    assert_eq!(report["invocation"]["kind"], "Processor");

    let changed: Vec<String> =
        serde_json::from_value(report["invocation"]["changed_facet_keys"].clone())?;
    assert!(changed.contains(&changed_key));

    let rw_keys: Vec<String> = serde_json::from_value(report["rw_facet_keys"].clone())?;
    assert!(rw_keys.contains(&"org.example.daybook.labelgeneric/main".to_string()));

    let ro_keys: Vec<String> = serde_json::from_value(report["ro_facet_keys"].clone())?;
    assert!(ro_keys.is_empty(), "minimal should have no ro facet keys");

    let rw_config: Vec<String> =
        serde_json::from_value(report["rw_config_facet_keys"].clone())?;
    assert!(rw_config.is_empty());

    let ro_config: Vec<String> =
        serde_json::from_value(report["ro_config_facet_keys"].clone())?;
    assert!(ro_config.is_empty());

    let cmd_urls: Vec<String> = serde_json::from_value(report["command_invoke_urls"].clone())?;
    assert!(cmd_urls.is_empty());

    let sqlite_conns: Vec<String> =
        serde_json::from_value(report["sqlite_connections"].clone())?;
    assert!(sqlite_conns.contains(&"@daybook/test/capability-report".to_string()));

    test_cx.stop().await?;
    Ok(())
}
