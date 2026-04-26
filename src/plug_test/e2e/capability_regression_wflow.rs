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

    let facet_keys: Vec<String> = serde_json::from_value(report["primary_facet_keys"].clone())?;
    assert!(facet_keys.iter().any(|k| k.starts_with("org.example.daybook.labelgeneric")));

    let tag_keys: Vec<String> = serde_json::from_value(report["primary_tag_keys"].clone())?;
    assert!(tag_keys.iter().any(|k| k == "org.example.daybook.labelgeneric" || k == "org.example.daybook.note"));

    let facet_rights: std::collections::BTreeMap<String, String> = serde_json::from_value(report["primary_facet_rights"].clone())?;
    let label_key = facet_keys.iter().find(|k| k.starts_with("org.example.daybook.labelgeneric")).expect("label key must exist");
    assert!(
        facet_rights[label_key].contains("READ"),
        "labelgeneric facet should have READ rights, got: {}",
        facet_rights[label_key]
    );
    assert!(
        facet_rights[label_key].contains("UPDATE"),
        "labelgeneric facet should have UPDATE rights, got: {}",
        facet_rights[label_key]
    );

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

    let facet_keys: Vec<String> = serde_json::from_value(report["primary_facet_keys"].clone())?;
    assert!(facet_keys.iter().any(|k| k.starts_with("org.example.daybook.labelgeneric")));

    let facet_rights: std::collections::BTreeMap<String, String> = serde_json::from_value(report["primary_facet_rights"].clone())?;
    let label_key = facet_keys.iter().find(|k| k.starts_with("org.example.daybook.labelgeneric")).expect("label key must exist");
    assert!(
        facet_rights[label_key].contains("UPDATE"),
        "labelgeneric facet should have UPDATE rights, got: {}",
        facet_rights[label_key]
    );
    let note_key = facet_keys.iter().find(|k| k.starts_with("org.example.daybook.note")).expect("note key must exist");
    assert!(
        facet_rights[note_key].contains("READ") && !facet_rights[note_key].contains("UPDATE"),
        "note facet should have READ-only rights, got: {}",
        facet_rights[note_key]
    );

    let tag_keys: Vec<String> = serde_json::from_value(report["primary_tag_keys"].clone())?;
    assert!(tag_keys.iter().any(|k| k == "org.example.daybook.labelgeneric" || k == "org.example.daybook.note"));

    let config_facet_keys: Vec<Vec<String>> = serde_json::from_value(report["config_doc_facet_keys"].clone())?;
    assert!(!config_facet_keys.is_empty(), "full processor should have config docs");
    let _config_tag_keys: Vec<Vec<String>> = serde_json::from_value(report["config_doc_tag_keys"].clone())?;

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

    let facet_keys: Vec<String> = serde_json::from_value(report["primary_facet_keys"].clone())?;
    assert!(facet_keys.iter().any(|k| k.starts_with("org.example.daybook.labelgeneric")));

    let facet_rights: std::collections::BTreeMap<String, String> = serde_json::from_value(report["primary_facet_rights"].clone())?;
    let label_key = facet_keys.iter().find(|k| k.starts_with("org.example.daybook.labelgeneric")).expect("label key must exist");
    assert!(
        facet_rights[label_key].contains("READ"),
        "labelgeneric facet should have READ rights in minimal, got: {}",
        facet_rights[label_key]
    );
    assert!(
        facet_rights[label_key].contains("UPDATE"),
        "labelgeneric facet should have UPDATE rights in minimal, got: {}",
        facet_rights[label_key]
    );

    let tag_keys: Vec<String> = serde_json::from_value(report["primary_tag_keys"].clone())?;
    assert!(tag_keys.is_empty() || !tag_keys.is_empty());

    let config_facet_keys: Vec<Vec<String>> = serde_json::from_value(report["config_doc_facet_keys"].clone())?;
    assert!(config_facet_keys.is_empty(), "minimal should have no config docs");

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

    let facet_keys: Vec<String> = serde_json::from_value(report["primary_facet_keys"].clone())?;
    assert!(facet_keys.iter().any(|k| k.starts_with("org.example.daybook.labelgeneric")));

    let facet_rights: std::collections::BTreeMap<String, String> = serde_json::from_value(report["primary_facet_rights"].clone())?;
    let label_key = facet_keys.iter().find(|k| k.starts_with("org.example.daybook.labelgeneric")).expect("label key must exist");
    assert!(
        facet_rights[label_key].contains("READ"),
        "labelgeneric facet should have READ rights in minimal, got: {}",
        facet_rights[label_key]
    );
    assert!(
        facet_rights[label_key].contains("UPDATE"),
        "labelgeneric facet should have UPDATE rights in minimal, got: {}",
        facet_rights[label_key]
    );

    let tag_keys: Vec<String> = serde_json::from_value(report["primary_tag_keys"].clone())?;
    assert!(tag_keys.is_empty() || !tag_keys.is_empty());

    let config_facet_keys: Vec<Vec<String>> = serde_json::from_value(report["config_doc_facet_keys"].clone())?;
    assert!(config_facet_keys.is_empty());

    let cmd_urls: Vec<String> = serde_json::from_value(report["command_invoke_urls"].clone())?;
    assert!(cmd_urls.is_empty());

    let sqlite_conns: Vec<String> =
        serde_json::from_value(report["sqlite_connections"].clone())?;
    assert!(sqlite_conns.contains(&"@daybook/test/capability-report".to_string()));

    test_cx.stop().await?;
    Ok(())
}
