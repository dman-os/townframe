use api_utils_rs::prelude::*;
use daybook_types::doc::{AddDocArgs, FacetKey, FacetRaw, WellKnownFacet, WellKnownFacetTag};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqlitePool};
use std::str::FromStr;

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
        .get_with_heads(doc_id, &daybook_types::doc::BranchPath::from("main"), None)
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
                invocation: if changed_facet_keys.is_empty() {
                    daybook_core::rt::dispatch::RoutineInvocation::Command
                } else {
                    daybook_core::rt::dispatch::RoutineInvocation::Processor(
                        daybook_core::rt::dispatch::ProcessorInvocation {
                            trigger_doc_id: doc_id.clone(),
                            changed_facet_keys: changed_facet_keys.clone(),
                        },
                    )
                },
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

async fn fetch_capability_report(db_pool: &SqlitePool, doc_id: &str) -> Res<serde_json::Value> {
    let summary_json: String =
        sqlx::query_scalar("SELECT summary_json FROM capability_report WHERE doc_id = ?1")
            .bind(doc_id)
            .fetch_one(db_pool)
            .await
            .wrap_err_with(|| format!("no capability_report row for doc_id={doc_id}"))?;

    Ok(serde_json::from_str(&summary_json)?)
}

async fn fetch_capability_report_v2(
    db_pool: &SqlitePool,
    doc_id: &str,
    test_name: &str,
) -> Res<serde_json::Value> {
    let summary_json: String = sqlx::query_scalar(
        "SELECT summary_json FROM capability_report_v2 WHERE doc_id = ?1 AND test_name = ?2",
    )
    .bind(doc_id)
    .bind(test_name)
    .fetch_one(db_pool)
    .await
    .wrap_err_with(|| {
        format!("no capability_report_v2 row for doc_id={doc_id} test_name={test_name}")
    })?;

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

async fn setup_and_dispatch_case(
    test_name: &'static str,
    routine_name: &str,
    changed_facet_keys: Vec<String>,
) -> Res<(daybook_core::test_support::DaybookTestContext, String)> {
    let test_cx = daybook_core::test_support::test_cx(test_name).await?;
    super::common::import_test_plug_oci(&test_cx).await?;

    let doc_id = setup_doc(&test_cx).await?;
    dispatch_and_wait(&test_cx, routine_name, &doc_id, changed_facet_keys).await?;
    Ok((test_cx, doc_id))
}

fn assert_config_tag_rights(
    tag_keys: &[Vec<String>],
    tag_rights: &[std::collections::BTreeMap<String, String>],
    doc_index: usize,
    tag: &str,
    expect_update: bool,
    context: &str,
) {
    let tag_key = tag_keys[doc_index]
        .iter()
        .find(|k| k.as_str() == tag)
        .unwrap_or_else(|| panic!("{context} tag must exist"));
    let rights = &tag_rights[doc_index][tag_key];
    assert!(
        rights.contains("READ"),
        "{context} should have READ rights, got: {rights}"
    );
    if expect_update {
        assert!(
            rights.contains("UPDATE"),
            "{context} should have READ+UPDATE, got: {rights}"
        );
    } else {
        assert!(
            !rights.contains("UPDATE"),
            "{context} should have READ-only, got: {rights}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_full_command_capability_report() -> Res<()> {
    let (test_cx, doc_id) =
        setup_and_dispatch_case("cap_reg_full_cmd", "report-full-command", vec![]).await?;

    let db_pool = open_plug_test_local_state(&test_cx).await?;
    let report = fetch_capability_report(&db_pool, &doc_id).await?;

    assert_eq!(report["invocation"]["kind"], "Command");

    let facet_keys: Vec<String> = serde_json::from_value(report["primary_facet_keys"].clone())?;
    assert!(facet_keys
        .iter()
        .any(|k| k.starts_with("org.example.daybook.labelgeneric")));

    let tag_keys: Vec<String> = serde_json::from_value(report["primary_tag_keys"].clone())?;
    assert!(tag_keys
        .iter()
        .any(|k| k == "org.example.daybook.labelgeneric" || k == "org.example.daybook.note"));

    let facet_rights: std::collections::BTreeMap<String, String> =
        serde_json::from_value(report["primary_facet_rights"].clone())?;
    let label_key = facet_keys
        .iter()
        .find(|k| k.starts_with("org.example.daybook.labelgeneric"))
        .expect("label key must exist");
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

    let config_facet_keys: Vec<Vec<String>> =
        serde_json::from_value(report["config_doc_facet_keys"].clone())?;
    assert!(
        !config_facet_keys.is_empty(),
        "full command should have config docs"
    );
    let config_tag_keys: Vec<Vec<String>> =
        serde_json::from_value(report["config_doc_tag_keys"].clone())?;
    assert!(
        !config_tag_keys.is_empty(),
        "full command should have config doc tags"
    );

    let config_tag_rights: Vec<std::collections::BTreeMap<String, String>> =
        serde_json::from_value(report["config_doc_tag_rights"].clone())?;
    assert!(
        !config_tag_rights.is_empty(),
        "full command should have config doc tag rights"
    );

    assert_config_tag_rights(
        &config_tag_keys,
        &config_tag_rights,
        0,
        "org.example.test.config",
        true,
        "config tag",
    );
    assert_config_tag_rights(
        &config_tag_keys,
        &config_tag_rights,
        0,
        "org.example.test.config-ro",
        false,
        "config-ro tag",
    );

    let sqlite_conns: Vec<String> = serde_json::from_value(report["sqlite_connections"].clone())?;
    assert!(sqlite_conns.contains(&"@daybook/test/capability-report".to_string()));

    test_cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_full_processor_capability_report() -> Res<()> {
    let changed_key = "org.example.daybook.note/main".to_string();
    let (test_cx, doc_id) = setup_and_dispatch_case(
        "cap_reg_full_proc",
        "report-full-processor",
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
    assert!(facet_keys
        .iter()
        .any(|k| k.starts_with("org.example.daybook.labelgeneric")));

    let facet_rights: std::collections::BTreeMap<String, String> =
        serde_json::from_value(report["primary_facet_rights"].clone())?;
    let label_key = facet_keys
        .iter()
        .find(|k| k.starts_with("org.example.daybook.labelgeneric"))
        .expect("label key must exist");
    assert!(
        facet_rights[label_key].contains("UPDATE"),
        "labelgeneric facet should have UPDATE rights, got: {}",
        facet_rights[label_key]
    );
    let note_key = facet_keys
        .iter()
        .find(|k| k.starts_with("org.example.daybook.note"))
        .expect("note key must exist");
    assert!(
        facet_rights[note_key].contains("READ") && !facet_rights[note_key].contains("UPDATE"),
        "note facet should have READ-only rights, got: {}",
        facet_rights[note_key]
    );

    let tag_keys: Vec<String> = serde_json::from_value(report["primary_tag_keys"].clone())?;
    assert!(tag_keys
        .iter()
        .any(|k| k == "org.example.daybook.labelgeneric" || k == "org.example.daybook.note"));

    let config_facet_keys: Vec<Vec<String>> =
        serde_json::from_value(report["config_doc_facet_keys"].clone())?;
    assert!(
        !config_facet_keys.is_empty(),
        "full processor should have config docs"
    );
    let config_tag_keys: Vec<Vec<String>> =
        serde_json::from_value(report["config_doc_tag_keys"].clone())?;
    assert!(
        !config_tag_keys.is_empty(),
        "full processor should have config doc tags"
    );

    let config_facet_rights: Vec<std::collections::BTreeMap<String, String>> =
        serde_json::from_value(report["config_doc_facet_rights"].clone())?;
    let config_tag_rights: Vec<std::collections::BTreeMap<String, String>> =
        serde_json::from_value(report["config_doc_tag_rights"].clone())?;
    assert!(
        !config_facet_rights.is_empty(),
        "full processor should have config doc facet rights"
    );
    assert!(
        !config_tag_rights.is_empty(),
        "full processor should have config doc tag rights"
    );

    assert_config_tag_rights(
        &config_tag_keys,
        &config_tag_rights,
        0,
        "org.example.test.config",
        true,
        "config tag",
    );
    assert_config_tag_rights(
        &config_tag_keys,
        &config_tag_rights,
        0,
        "org.example.test.config-ro",
        false,
        "config-ro tag",
    );

    let cmd_urls: Vec<String> = serde_json::from_value(report["command_invoke_urls"].clone())?;
    assert!(
        cmd_urls.is_empty(),
        "processor should not have command invoke tokens"
    );

    let sqlite_conns: Vec<String> = serde_json::from_value(report["sqlite_connections"].clone())?;
    assert!(sqlite_conns.contains(&"@daybook/test/capability-report".to_string()));

    test_cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_minimal_command_capability_report() -> Res<()> {
    let (test_cx, doc_id) =
        setup_and_dispatch_case("cap_reg_min_cmd", "report-minimal-command", vec![]).await?;

    let db_pool = open_plug_test_local_state(&test_cx).await?;
    let report = fetch_capability_report(&db_pool, &doc_id).await?;

    assert_eq!(report["invocation"]["kind"], "Command");

    let facet_keys: Vec<String> = serde_json::from_value(report["primary_facet_keys"].clone())?;
    assert!(facet_keys
        .iter()
        .any(|k| k.starts_with("org.example.daybook.labelgeneric")));

    let facet_rights: std::collections::BTreeMap<String, String> =
        serde_json::from_value(report["primary_facet_rights"].clone())?;
    let label_key = facet_keys
        .iter()
        .find(|k| k.starts_with("org.example.daybook.labelgeneric"))
        .expect("label key must exist");
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
    assert!(tag_keys.is_empty(), "minimal should have no primary tags");

    let config_facet_keys: Vec<Vec<String>> =
        serde_json::from_value(report["config_doc_facet_keys"].clone())?;
    assert!(
        config_facet_keys.is_empty(),
        "minimal should have no config docs"
    );

    let cmd_urls: Vec<String> = serde_json::from_value(report["command_invoke_urls"].clone())?;
    assert!(
        cmd_urls.is_empty(),
        "minimal should have no command invoke tokens"
    );

    let sqlite_conns: Vec<String> = serde_json::from_value(report["sqlite_connections"].clone())?;
    assert!(sqlite_conns.contains(&"@daybook/test/capability-report".to_string()));

    test_cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_minimal_processor_capability_report() -> Res<()> {
    let changed_key = "org.example.daybook.note/main".to_string();
    let (test_cx, doc_id) = setup_and_dispatch_case(
        "cap_reg_min_proc",
        "report-minimal-processor",
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
    assert!(facet_keys
        .iter()
        .any(|k| k.starts_with("org.example.daybook.labelgeneric")));

    let facet_rights: std::collections::BTreeMap<String, String> =
        serde_json::from_value(report["primary_facet_rights"].clone())?;
    let label_key = facet_keys
        .iter()
        .find(|k| k.starts_with("org.example.daybook.labelgeneric"))
        .expect("label key must exist");
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
    assert!(tag_keys.is_empty(), "minimal should have no primary tags");

    let config_facet_keys: Vec<Vec<String>> =
        serde_json::from_value(report["config_doc_facet_keys"].clone())?;
    assert!(config_facet_keys.is_empty());

    let cmd_urls: Vec<String> = serde_json::from_value(report["command_invoke_urls"].clone())?;
    assert!(cmd_urls.is_empty());

    let sqlite_conns: Vec<String> = serde_json::from_value(report["sqlite_connections"].clone())?;
    assert!(sqlite_conns.contains(&"@daybook/test/capability-report".to_string()));

    test_cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_downscope_capability() -> Res<()> {
    let (test_cx, doc_id) =
        setup_and_dispatch_case("cap_reg_downscope", "test-downscope", vec![]).await?;

    let db_pool = open_plug_test_local_state(&test_cx).await?;
    let report = fetch_capability_report_v2(&db_pool, &doc_id, "test_downscope").await?;

    assert!(
        report["has_read"].as_bool().unwrap_or(false),
        "cloned token should have READ"
    );
    assert!(
        !report["has_update"].as_bool().unwrap_or(true),
        "cloned token should not have UPDATE"
    );
    assert!(
        report["update_denied"].as_bool().unwrap_or(false),
        "update on read-only clone should be denied"
    );

    test_cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_denied_update_capability() -> Res<()> {
    let (test_cx, doc_id) =
        setup_and_dispatch_case("cap_reg_denied", "test-denied-update", vec![]).await?;

    let db_pool = open_plug_test_local_state(&test_cx).await?;
    let report = fetch_capability_report_v2(&db_pool, &doc_id, "test_denied_update").await?;

    assert!(
        report["update_denied"].as_bool().unwrap_or(false),
        "update on read-only token should be denied"
    );

    test_cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_acl_aggregate_capability() -> Res<()> {
    let (test_cx, doc_id) =
        setup_and_dispatch_case("cap_reg_acl_agg", "test-acl-aggregate", vec![]).await?;

    let db_pool = open_plug_test_local_state(&test_cx).await?;
    let report = fetch_capability_report_v2(&db_pool, &doc_id, "test_acl_aggregate").await?;

    assert!(
        report["tag_has_read"].as_bool().unwrap_or(false),
        "tag token should have READ from aggregated ACLs"
    );
    assert!(
        report["tag_has_update"].as_bool().unwrap_or(false),
        "tag token should have UPDATE from aggregated ACLs"
    );
    assert!(
        report["facet_has_read"].as_bool().unwrap_or(false),
        "facet token should have READ from aggregated ACLs"
    );
    assert!(
        report["facet_has_update"].as_bool().unwrap_or(false),
        "facet token should have UPDATE from aggregated ACLs"
    );

    test_cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_facet_capability() -> Res<()> {
    let (test_cx, doc_id) =
        setup_and_dispatch_case("cap_reg_create", "test-create-facet", vec![]).await?;

    let db_pool = open_plug_test_local_state(&test_cx).await?;
    let report = fetch_capability_report_v2(&db_pool, &doc_id, "test_create_facet").await?;

    assert_eq!(
        report["created_key"].as_str().unwrap_or(""),
        "org.example.test.createable/new-key"
    );
    let rights = report["created_rights"].as_str().unwrap_or("");
    assert!(
        rights.contains("CREATE"),
        "created facet token should have CREATE, got: {rights}"
    );
    assert!(
        !rights.contains("READ"),
        "created facet token should not have READ, got: {rights}"
    );
    assert!(
        !rights.contains("UPDATE"),
        "created facet token should not have UPDATE, got: {rights}"
    );

    test_cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_create_token_capability() -> Res<()> {
    let (test_cx, doc_id) =
        setup_and_dispatch_case("cap_reg_get_create", "test-get-create-token", vec![]).await?;

    let db_pool = open_plug_test_local_state(&test_cx).await?;
    let report = fetch_capability_report_v2(&db_pool, &doc_id, "test_get_create_token").await?;

    assert_eq!(
        report["ctoken_key"].as_str().unwrap_or(""),
        "org.example.test.createable/another-key"
    );
    assert_eq!(
        report["created_key"].as_str().unwrap_or(""),
        "org.example.test.createable/another-key"
    );
    let rights = report["created_rights"].as_str().unwrap_or("");
    assert!(
        rights.contains("CREATE"),
        "created facet token should have CREATE, got: {rights}"
    );
    assert!(
        !rights.contains("READ"),
        "created facet token should not have READ, got: {rights}"
    );
    assert!(
        !rights.contains("UPDATE"),
        "created facet token should not have UPDATE, got: {rights}"
    );

    test_cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_facet_capability() -> Res<()> {
    let (test_cx, doc_id) =
        setup_and_dispatch_case("cap_reg_delete", "test-delete-facet", vec![]).await?;

    let db_pool = open_plug_test_local_state(&test_cx).await?;
    let report = fetch_capability_report_v2(&db_pool, &doc_id, "test_delete_facet").await?;

    assert!(
        report["deleted"].as_bool().unwrap_or(false),
        "delete_facet should succeed"
    );

    test_cx.stop().await?;
    Ok(())
}
