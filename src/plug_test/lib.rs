#![recursion_limit = "256"]

mod interlude {}

use std::sync::Arc;

use daybook_types::manifest::{
    CommandDeets, CommandManifest, DocPredicateClause, FacetDependencyManifest,
    PlugDependencyManifest, PlugManifest, RoutineDocAcl, RoutineFacetAccess, RoutineImpl,
    RoutineManifest,
};

#[cfg(target_arch = "wasm32")]
mod wit {
    wit_bindgen::generate!({
        path: "wit",
        world: "bundle",
        with: {
            "wasi:keyvalue/store@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::store,
            "wasi:keyvalue/atomics@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::atomics,
            "wasi:logging/logging@0.1.0-draft": api_utils_rs::wit::wasi::logging::logging,
            "wasmcloud:postgres/types@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::types,
            "wasmcloud:postgres/query@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::query,
            "wasi:io/poll@0.2.6": api_utils_rs::wit::wasi::io::poll,
            "wasi:clocks/monotonic-clock@0.2.6": api_utils_rs::wit::wasi::clocks::monotonic_clock,
            "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,
            "wasi:config/runtime@0.2.0-draft": api_utils_rs::wit::wasi::config::runtime,

            "townframe:api-utils/utils": api_utils_rs::wit::utils,
            "townframe:wflow/types": wflow_sdk::wit::townframe::wflow::types,
            "townframe:wflow/host": wflow_sdk::wit::townframe::wflow::host,
            "townframe:wflow/bundle": generate,
            "townframe:mltools/ocr": generate,
            "townframe:mltools/embed": generate,
            "townframe:sql/types": generate,

            "townframe:daybook-types/doc": generate,
            "townframe:daybook/types": generate,
            "townframe:daybook/drawer": generate,
            "townframe:daybook/capabilities": generate,
            "townframe:daybook/facet-routine": generate,
            "townframe:daybook/sqlite-connection": generate,
            "townframe:daybook/mltools-ocr": generate,
            "townframe:daybook/mltools-embed": generate,
            "townframe:daybook/mltools-image-tools": generate,
            "townframe:daybook/mltools-llm-chat": generate,
        }
    });
}

#[cfg(target_arch = "wasm32")]
mod wasm_runtime {
    use crate::interlude::*;
    use crate::wit;
    use crate::wit::exports::townframe::wflow::bundle::JobResult;
    use daybook_pdk::{InvokeCommandAccepted, InvokeCommandRequest, InvokeCommandStatus};
    use wflow_sdk::{JobErrorX, Json, WflowCtx};

    wit::export!(Component with_types_in wit);

    struct Component;

    #[derive(Debug, serde::Deserialize)]
    struct ChildArgs {
        source: String,
    }

    fn find_facet_token_with_rights<'a>(
        args: &'a crate::wit::townframe::daybook::facet_routine::FacetRoutineArgs,
        key: &str,
        required_right: crate::wit::townframe::daybook::capabilities::FacetRights,
    ) -> Result<&'a crate::wit::townframe::daybook::capabilities::FacetToken, JobErrorX> {
        args.primary_doc
            .facets
            .iter()
            .find(|t| t.key() == key && t.rights().contains(required_right))
            .ok_or_else(|| {
                JobErrorX::Terminal(ferr!(
                    "facet token '{}' with required rights not found",
                    key
                ))
            })
    }

    fn find_facet_token<'a>(
        args: &'a crate::wit::townframe::daybook::facet_routine::FacetRoutineArgs,
        key: &str,
    ) -> Result<&'a crate::wit::townframe::daybook::capabilities::FacetToken, JobErrorX> {
        args.primary_doc
            .facets
            .iter()
            .find(|t| t.key() == key)
            .ok_or_else(|| JobErrorX::Terminal(ferr!("facet token '{}' not found", key)))
    }

    fn find_command_token<'a>(
        args: &'a crate::wit::townframe::daybook::facet_routine::FacetRoutineArgs,
        command_name: &str,
    ) -> Result<&'a crate::wit::townframe::daybook::capabilities::CommandInvokeToken, JobErrorX>
    {
        args.command_invoke_tokens
            .iter()
            .find(|(url, _)| url.ends_with(&format!("/{}", command_name.trim_start_matches('/'))))
            .map(|(_, token)| token)
            .ok_or_else(|| {
                JobErrorX::Terminal(ferr!("missing command invoke token for {command_name}"))
            })
    }

    fn update_label(
        cx: &mut WflowCtx,
        token: &crate::wit::townframe::daybook::capabilities::FacetToken,
        value: &str,
    ) -> Result<(), JobErrorX> {
        use daybook_types::doc::WellKnownFacet;
        cx.effect(|| {
            let facet: daybook_types::doc::FacetRaw =
                WellKnownFacet::LabelGeneric(value.into()).into();
            let facet_json = serde_json::to_string(&facet).expect(ERROR_JSON);
            token
                .update(&facet_json)
                .map_err(|err| JobErrorX::Terminal(ferr!("error updating label facet: {err:?}")))?
                .map_err(|err| JobErrorX::Terminal(ferr!("update doc error: {err:?}")))?;
            Ok(Json(()))
        })?;
        Ok(())
    }

    fn write_report_v2(
        args: &crate::wit::townframe::daybook::facet_routine::FacetRoutineArgs,
        test_name: &str,
        summary: &serde_json::Value,
    ) -> Result<(), JobErrorX> {
        use crate::wit::townframe::sql::types::SqlValue;
        let local_state_key = "@daybook/test/capability-report";
        let sqlite_connection = args
            .sqlite_connections
            .iter()
            .find(|(key, _)| key == local_state_key)
            .map(|(_, conn)| conn)
            .ok_or_else(|| {
                JobErrorX::Terminal(ferr!("missing sqlite connection '{local_state_key}'"))
            })?;

        sqlite_connection
            .query_batch(
                "CREATE TABLE IF NOT EXISTS capability_report_v2 (doc_id TEXT NOT NULL, test_name TEXT NOT NULL, summary_json TEXT NOT NULL, PRIMARY KEY (doc_id, test_name))"
            )
            .map_err(|err| JobErrorX::Terminal(ferr!("error creating capability_report_v2 table: {err:?}")))?;

        sqlite_connection
            .query(
                "INSERT OR REPLACE INTO capability_report_v2 (doc_id, test_name, summary_json) VALUES (?1, ?2, ?3)",
                &[
                    SqlValue::Text(args.doc_id.clone()),
                    SqlValue::Text(test_name.into()),
                    SqlValue::Text(summary.to_string()),
                ],
            )
            .map_err(|err| JobErrorX::Terminal(ferr!("error writing capability report: {err:?}")))?;
        Ok(())
    }

    fn invoke_child_and_wait(
        cx: &mut WflowCtx,
        token: &crate::wit::townframe::daybook::capabilities::CommandInvokeToken,
        request_id: &str,
        expect_failed: bool,
    ) -> Result<(), JobErrorX> {
        let request = InvokeCommandRequest {
            request_id: request_id.to_string(),
            args_json: serde_json::json!({ "source": "plug-test-parent" }).to_string(),
        };
        let _accepted: InvokeCommandAccepted =
            daybook_pdk::invoke_command_effect(cx, &request, |req| {
                let req_json = serde_json::to_string(req).expect(ERROR_JSON);
                let accepted_json = token
                    .invoke(&req_json)
                    .map_err(|err| JobErrorX::Terminal(ferr!("invoke command failed: {err:?}")))?;
                let accepted = serde_json::from_str::<InvokeCommandAccepted>(&accepted_json)
                    .map_err(|err| {
                        JobErrorX::Terminal(ferr!("invalid invoke accepted JSON: {err}"))
                    })?;
                Ok(Json(accepted))
            })?;

        let reply = daybook_pdk::wait_command_reply(cx)?;
        if reply.request_id != request_id {
            return Err(JobErrorX::Terminal(ferr!(
                "mismatched reply request_id: expected {request_id}, got {}",
                reply.request_id
            )));
        }

        match (expect_failed, reply.status) {
            (false, InvokeCommandStatus::Succeeded) => Ok(()),
            (true, InvokeCommandStatus::Failed) if reply.error_json.is_some() => Ok(()),
            (true, status) => Err(JobErrorX::Terminal(ferr!(
                "expected failed status with error_json, got {status:?}"
            ))),
            (false, status) => Err(JobErrorX::Terminal(ferr!(
                "expected succeeded status, got {status:?}"
            ))),
        }
    }

    fn invoke_child_success(cx: &mut WflowCtx) -> Result<(), JobErrorX> {
        let args = crate::wit::townframe::daybook::facet_routine::get_args();
        let label_key =
            daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::LabelGeneric)
                .to_string();
        let working = find_facet_token_with_rights(
            &args,
            &label_key,
            crate::wit::townframe::daybook::capabilities::FacetRights::UPDATE,
        )?;
        update_label(cx, working, "invoke-child-success-started")?;
        let token = find_command_token(&args, "/child-success")?;
        invoke_child_and_wait(cx, token, "req-child-success", false)?;
        Ok(())
    }

    fn invoke_child_failure(cx: &mut WflowCtx) -> Result<(), JobErrorX> {
        let args = crate::wit::townframe::daybook::facet_routine::get_args();
        let label_key =
            daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::LabelGeneric)
                .to_string();
        let working = find_facet_token_with_rights(
            &args,
            &label_key,
            crate::wit::townframe::daybook::capabilities::FacetRights::UPDATE,
        )?;
        update_label(cx, working, "invoke-child-failure-started")?;
        let token = find_command_token(&args, "/child-failure")?;
        invoke_child_and_wait(cx, token, "req-child-failure", true)?;
        Ok(())
    }

    fn child_success(cx: &mut WflowCtx, args: ChildArgs) -> Result<(), JobErrorX> {
        if args.source.is_empty() {
            return Err(JobErrorX::Terminal(ferr!(
                "child-success received empty source"
            )));
        }
        let routine_args = crate::wit::townframe::daybook::facet_routine::get_args();
        let label_key =
            daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::LabelGeneric)
                .to_string();
        let working = find_facet_token_with_rights(
            &routine_args,
            &label_key,
            crate::wit::townframe::daybook::capabilities::FacetRights::UPDATE,
        )?;
        update_label(cx, working, "child-success-ran")?;
        Ok(())
    }

    fn child_failure(cx: &mut WflowCtx, args: ChildArgs) -> Result<(), JobErrorX> {
        let routine_args = crate::wit::townframe::daybook::facet_routine::get_args();
        let label_key =
            daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::LabelGeneric)
                .to_string();
        let working = find_facet_token_with_rights(
            &routine_args,
            &label_key,
            crate::wit::townframe::daybook::capabilities::FacetRights::UPDATE,
        )?;
        update_label(cx, working, "child-failure-ran")?;
        Err(JobErrorX::Terminal(ferr!(
            "child-failure from source '{}'",
            args.source
        )))
    }

    fn test_downscope(_cx: &mut WflowCtx) -> Result<(), JobErrorX> {
        let args = crate::wit::townframe::daybook::facet_routine::get_args();
        let label_key =
            daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::LabelGeneric)
                .to_string();
        let token = find_facet_token_with_rights(
            &args,
            &label_key,
            crate::wit::townframe::daybook::capabilities::FacetRights::UPDATE,
        )?;

        let read_only = token
            .clone(Some(
                crate::wit::townframe::daybook::capabilities::FacetRights::READ,
            ))
            .map_err(|err| JobErrorX::Terminal(ferr!("clone denied: {err:?}")))?;

        let has_read = read_only
            .rights()
            .contains(crate::wit::townframe::daybook::capabilities::FacetRights::READ);
        let has_update = read_only
            .rights()
            .contains(crate::wit::townframe::daybook::capabilities::FacetRights::UPDATE);

        let facet_json = serde_json::to_string(&daybook_types::doc::WellKnownFacet::LabelGeneric(
            "downscope-read-only".into(),
        ))
        .expect(ERROR_JSON);
        let update_denied = matches!(read_only.update(&facet_json), Err(_));

        let summary = serde_json::json!({
            "has_read": has_read,
            "has_update": has_update,
            "update_denied": update_denied,
        });
        write_report_v2(&args, "test_downscope", &summary)?;
        Ok(())
    }

    fn test_denied_update(_cx: &mut WflowCtx) -> Result<(), JobErrorX> {
        let args = crate::wit::townframe::daybook::facet_routine::get_args();
        let label_key =
            daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::LabelGeneric)
                .to_string();
        let token = find_facet_token(&args, &label_key)?;

        let facet_json = serde_json::to_string(&daybook_types::doc::WellKnownFacet::LabelGeneric(
            "should-fail".into(),
        ))
        .expect(ERROR_JSON);
        let update_denied = matches!(token.update(&facet_json), Err(_));

        let summary = serde_json::json!({ "update_denied": update_denied });
        write_report_v2(&args, "test_denied_update", &summary)?;
        Ok(())
    }

    fn test_acl_aggregate(_cx: &mut WflowCtx) -> Result<(), JobErrorX> {
        let args = crate::wit::townframe::daybook::facet_routine::get_args();

        let tag_token = args
            .primary_doc
            .tags
            .iter()
            .find(|t| t.tag() == "org.example.daybook.labelgeneric")
            .ok_or_else(|| JobErrorX::Terminal(ferr!("labelgeneric tag token not found")))?;
        let tag_has_read = tag_token
            .rights()
            .contains(crate::wit::townframe::daybook::capabilities::FacetRights::READ);
        let tag_has_update = tag_token
            .rights()
            .contains(crate::wit::townframe::daybook::capabilities::FacetRights::UPDATE);

        let label_key =
            daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::LabelGeneric)
                .to_string();
        let facet_token = find_facet_token_with_rights(
            &args,
            &label_key,
            crate::wit::townframe::daybook::capabilities::FacetRights::READ,
        )?;
        let facet_has_read = facet_token
            .rights()
            .contains(crate::wit::townframe::daybook::capabilities::FacetRights::READ);
        let facet_has_update = facet_token
            .rights()
            .contains(crate::wit::townframe::daybook::capabilities::FacetRights::UPDATE);

        let summary = serde_json::json!({
            "tag_has_read": tag_has_read,
            "tag_has_update": tag_has_update,
            "facet_has_read": facet_has_read,
            "facet_has_update": facet_has_update,
        });
        write_report_v2(&args, "test_acl_aggregate", &summary)?;
        Ok(())
    }

    fn test_create_facet(_cx: &mut WflowCtx) -> Result<(), JobErrorX> {
        let args = crate::wit::townframe::daybook::facet_routine::get_args();
        let tag_token = args
            .primary_doc
            .tags
            .iter()
            .find(|t| t.tag() == "org.example.test.createable")
            .ok_or_else(|| JobErrorX::Terminal(ferr!("createable tag token not found")))?;

        if !tag_token
            .rights()
            .contains(crate::wit::townframe::daybook::capabilities::FacetRights::CREATE)
        {
            return Err(JobErrorX::Terminal(ferr!(
                "createable tag token missing CREATE right"
            )));
        }

        let new_facet = daybook_types::doc::WellKnownFacet::LabelGeneric("created-via-tag".into());
        let facet_json = serde_json::to_string(&new_facet).expect(ERROR_JSON);
        let created = tag_token
            .create("new-key", &facet_json)
            .map_err(|err| JobErrorX::Terminal(ferr!("create failed: {err:?}")))?;

        let summary = serde_json::json!({
            "created_key": created.key(),
            "created_rights": format!("{:?}", created.rights()),
        });
        write_report_v2(&args, "test_create_facet", &summary)?;
        Ok(())
    }

    fn test_get_create_token(_cx: &mut WflowCtx) -> Result<(), JobErrorX> {
        let args = crate::wit::townframe::daybook::facet_routine::get_args();
        let tag_token = args
            .primary_doc
            .tags
            .iter()
            .find(|t| t.tag() == "org.example.test.createable")
            .ok_or_else(|| JobErrorX::Terminal(ferr!("createable tag token not found")))?;

        let ctoken = tag_token
            .get_create_token("another-key")
            .map_err(|err| JobErrorX::Terminal(ferr!("get_create_token failed: {err:?}")))?;

        let new_facet =
            daybook_types::doc::WellKnownFacet::LabelGeneric("created-via-create-token".into());
        let facet_json = serde_json::to_string(&new_facet).expect(ERROR_JSON);
        let created = ctoken
            .create_facet(&facet_json)
            .map_err(|err| JobErrorX::Terminal(ferr!("create_facet failed: {err:?}")))?;

        let summary = serde_json::json!({
            "ctoken_key": ctoken.key(),
            "created_key": created.key(),
            "created_rights": format!("{:?}", created.rights()),
        });
        write_report_v2(&args, "test_get_create_token", &summary)?;
        Ok(())
    }

    fn test_delete_facet(_cx: &mut WflowCtx) -> Result<(), JobErrorX> {
        let args = crate::wit::townframe::daybook::facet_routine::get_args();
        let doc = &args.primary_doc.doc;
        let label_key =
            daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::LabelGeneric)
                .to_string();

        let token = doc
            .get_facet(&label_key)
            .map_err(|err| JobErrorX::Terminal(ferr!("get_facet error: {err:?}")))?
            .ok_or_else(|| JobErrorX::Terminal(ferr!("labelgeneric facet not found")))?;

        if !token
            .rights()
            .contains(crate::wit::townframe::daybook::capabilities::FacetRights::DELETE)
        {
            return Err(JobErrorX::Terminal(ferr!(
                "labelgeneric facet token missing DELETE right"
            )));
        }

        crate::wit::townframe::daybook::capabilities::delete_facet(token)
            .map_err(|err| JobErrorX::Terminal(ferr!("delete_facet failed: {err:?}")))?;

        let summary = serde_json::json!({ "deleted": true });
        write_report_v2(&args, "test_delete_facet", &summary)?;
        Ok(())
    }

    fn report_capabilities(_cx: &mut WflowCtx) -> Result<(), JobErrorX> {
        use crate::wit::townframe::daybook::facet_routine;
        use crate::wit::townframe::sql::types::SqlValue;

        let args = facet_routine::get_args();

        let invocation_kind = match &args.invocation {
            facet_routine::RoutineInvocation::Processor(proc) => {
                serde_json::json!({
                    "kind": "Processor",
                    "trigger_doc_id": proc.trigger_doc_id.clone(),
                    "changed_facet_keys": proc.changed_facet_keys.clone(),
                })
            }
            facet_routine::RoutineInvocation::Command => {
                serde_json::json!({ "kind": "Command" })
            }
        };

        let facet_keys_and_rights: Vec<(String, String)> = args
            .primary_doc
            .facets
            .iter()
            .map(|t| (t.key(), format!("{:?}", t.rights())))
            .collect();
        let tag_keys_and_rights: Vec<(String, String)> = args
            .primary_doc
            .tags
            .iter()
            .map(|t| (t.tag(), format!("{:?}", t.rights())))
            .collect();
        let config_doc_facet_keys_and_rights: Vec<Vec<(String, String)>> = args
            .config_docs
            .iter()
            .map(|cd| {
                cd.facets
                    .iter()
                    .map(|t| (t.key(), format!("{:?}", t.rights())))
                    .collect()
            })
            .collect();
        let config_doc_tag_keys_and_rights: Vec<Vec<(String, String)>> = args
            .config_docs
            .iter()
            .map(|cd| {
                cd.tags
                    .iter()
                    .map(|t| (t.tag(), format!("{:?}", t.rights())))
                    .collect()
            })
            .collect();

        let facet_keys: Vec<String> = facet_keys_and_rights
            .iter()
            .map(|(k, _)| k.clone())
            .collect();
        let tag_keys: Vec<String> = tag_keys_and_rights.iter().map(|(k, _)| k.clone()).collect();
        let config_doc_facet_keys: Vec<Vec<String>> = config_doc_facet_keys_and_rights
            .iter()
            .map(|v| v.iter().map(|(k, _)| k.clone()).collect())
            .collect();
        let config_doc_tag_keys: Vec<Vec<String>> = config_doc_tag_keys_and_rights
            .iter()
            .map(|v| v.iter().map(|(k, _)| k.clone()).collect())
            .collect();
        let facet_rights_map: std::collections::BTreeMap<String, String> =
            facet_keys_and_rights.into_iter().collect();
        let tag_rights_map: std::collections::BTreeMap<String, String> =
            tag_keys_and_rights.into_iter().collect();
        let config_facet_rights: Vec<std::collections::BTreeMap<String, String>> =
            config_doc_facet_keys_and_rights
                .into_iter()
                .map(|v| v.into_iter().collect())
                .collect();
        let config_tag_rights: Vec<std::collections::BTreeMap<String, String>> =
            config_doc_tag_keys_and_rights
                .into_iter()
                .map(|v| v.into_iter().collect())
                .collect();

        let summary = serde_json::json!({
            "invocation": invocation_kind,
            "primary_facet_keys": facet_keys,
            "primary_tag_keys": tag_keys,
            "primary_facet_rights": facet_rights_map,
            "primary_tag_rights": tag_rights_map,
            "config_doc_facet_keys": config_doc_facet_keys,
            "config_doc_tag_keys": config_doc_tag_keys,
            "config_doc_facet_rights": config_facet_rights,
            "config_doc_tag_rights": config_tag_rights,
            "command_invoke_urls": args.command_invoke_tokens.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
            "sqlite_connections": args.sqlite_connections.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        });

        let local_state_key = "@daybook/test/capability-report";
        let sqlite_connection = args
            .sqlite_connections
            .iter()
            .find(|(key, _)| key == local_state_key)
            .map(|(_, conn)| conn)
            .ok_or_else(|| {
                JobErrorX::Terminal(ferr!("missing sqlite connection '{local_state_key}'"))
            })?;

        sqlite_connection
            .query_batch(
                "CREATE TABLE IF NOT EXISTS capability_report (doc_id TEXT PRIMARY KEY, summary_json TEXT NOT NULL)"
            )
            .map_err(|err| JobErrorX::Terminal(ferr!("error creating capability_report table: {err:?}")))?;

        sqlite_connection
            .query(
                "INSERT OR REPLACE INTO capability_report (doc_id, summary_json) VALUES (?1, ?2)",
                &[
                    SqlValue::Text(args.doc_id.clone()),
                    SqlValue::Text(summary.to_string()),
                ],
            )
            .map_err(|err| {
                JobErrorX::Terminal(ferr!("error writing capability report: {err:?}"))
            })?;

        Ok(())
    }

    impl crate::wit::exports::townframe::wflow::bundle::Guest for Component {
        fn run(args: crate::wit::exports::townframe::wflow::bundle::RunArgs) -> JobResult {
            wflow_sdk::route_wflows!(args, {
                "invoke-child-success" => |cx, _args: serde_json::Value| invoke_child_success(cx),
                "invoke-child-failure" => |cx, _args: serde_json::Value| invoke_child_failure(cx),
                "child-success" => |cx, args: ChildArgs| child_success(cx, args),
                "child-failure" => |cx, args: ChildArgs| child_failure(cx, args),
                "report-capabilities" => |cx, _args: serde_json::Value| report_capabilities(cx),
                "test-downscope" => |cx, _args: serde_json::Value| test_downscope(cx),
                "test-denied-update" => |cx, _args: serde_json::Value| test_denied_update(cx),
                "test-acl-aggregate" => |cx, _args: serde_json::Value| test_acl_aggregate(cx),
                "test-create-facet" => |cx, _args: serde_json::Value| test_create_facet(cx),
                "test-get-create-token" => |cx, _args: serde_json::Value| test_get_create_token(cx),
                "test-delete-facet" => |cx, _args: serde_json::Value| test_delete_facet(cx),
            })
        }
    }
}

#[cfg(test)]
mod e2e;

pub fn plug_manifest() -> PlugManifest {
    use daybook_types::doc::WellKnownFacetTag;
    use daybook_types::manifest::FacetManifest;

    PlugManifest {
        namespace: "daybook".into(),
        name: "test".into(),
        version: "0.0.1".parse().unwrap(),
        title: "Daybook Test Plug".into(),
        desc: "Internal e2e test plug for command invocation".into(),
        local_states: [(
            "capability-report".into(),
            Arc::new(daybook_types::manifest::LocalStateManifest::SqliteFile {}),
        )]
        .into(),
        dependencies: [(
            "@daybook/core@v0.0.1".into(),
            PlugDependencyManifest {
                keys: vec![
                    FacetDependencyManifest {
                        key_tag: WellKnownFacetTag::LabelGeneric.into(),
                        value_schema: schemars::schema_for!(String),
                    },
                    FacetDependencyManifest {
                        key_tag: WellKnownFacetTag::Note.into(),
                        value_schema: schemars::schema_for!(daybook_types::doc::Note),
                    },
                ],
                local_states: vec![],
            }
            .into(),
        )]
        .into(),
        routines: [
            (
                "invoke-child-success".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "invoke-child-success".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: Some(daybook_types::doc::DEFAULT_FACET_ID.into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![],
                    command_invoke_acl: vec![daybook_pdk::build_command_url(
                        "@daybook/test",
                        "child-success",
                    )
                    .unwrap()],
                }),
            ),
            (
                "invoke-child-failure".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "invoke-child-failure".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: Some(daybook_types::doc::DEFAULT_FACET_ID.into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![],
                    command_invoke_acl: vec![daybook_pdk::build_command_url(
                        "@daybook/test",
                        "child-failure",
                    )
                    .unwrap()],
                }),
            ),
            (
                "child-success".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "child-success".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: Some(daybook_types::doc::DEFAULT_FACET_ID.into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "child-failure".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "child-failure".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: Some(daybook_types::doc::DEFAULT_FACET_ID.into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "report-full-command".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "report-capabilities".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: true,
                                write: true,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Note.into(),
                                key_id: None,
                                read: true,
                                write: false,
                                create: false,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: "org.example.test.config".into(),
                            key_id: None,
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        },
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: "org.example.test.config-ro".into(),
                            key_id: None,
                            read: true,
                            write: false,
                            create: false,
                            delete: false,
                        },
                    ],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![daybook_pdk::build_command_url(
                        "@daybook/test",
                        "child-success",
                    )
                    .unwrap()],
                }),
            ),
            (
                "report-full-processor".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "report-capabilities".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: true,
                                write: true,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::Note.into(),
                                key_id: None,
                                read: true,
                                write: false,
                                create: false,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: "org.example.test.config".into(),
                            key_id: None,
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        },
                        RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: "org.example.test.config-ro".into(),
                            key_id: None,
                            read: true,
                            write: false,
                            create: false,
                            delete: false,
                        },
                    ],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "report-minimal-command".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "report-capabilities".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: Some(daybook_types::doc::DEFAULT_FACET_ID.into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "report-minimal-processor".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "report-capabilities".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: Some(daybook_types::doc::DEFAULT_FACET_ID.into()),
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "test-downscope".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "test-downscope".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: None,
                            read: true,
                            write: true,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "test-denied-update".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "test-denied-update".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: None,
                            read: true,
                            write: false,
                            create: false,
                            delete: false,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "test-acl-aggregate".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "test-acl-aggregate".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: true,
                                write: false,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: false,
                                write: true,
                                create: false,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "test-create-facet".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "test-create-facet".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: true,
                                write: false,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: "org.example.test.createable".into(),
                                key_id: None,
                                read: false,
                                write: false,
                                create: true,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "test-get-create-token".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "test-get-create-token".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: WellKnownFacetTag::LabelGeneric.into(),
                                key_id: None,
                                read: true,
                                write: false,
                                create: false,
                                delete: false,
                            },
                            RoutineFacetAccess {
                                owner_plug_id: None,
                                tag: "org.example.test.createable".into(),
                                key_id: None,
                                read: false,
                                write: false,
                                create: true,
                                delete: false,
                            },
                        ],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
            (
                "test-delete-facet".into(),
                Arc::new(RoutineManifest {
                    r#impl: RoutineImpl::Wflow {
                        key: "test-delete-facet".into(),
                        bundle: "plug_test".into(),
                    },
                    doc_acls: vec![RoutineDocAcl {
                        doc_predicate: DocPredicateClause::HasTag(
                            WellKnownFacetTag::LabelGeneric.into(),
                        ),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: None,
                            read: true,
                            write: false,
                            create: false,
                            delete: true,
                        }],
                    }],
                    query_acls: vec![],
                    config_facet_acl: vec![],
                    local_state_acl: vec![daybook_types::manifest::RoutineLocalStateAccess {
                        plug_id: "@daybook/test".into(),
                        local_state_key: "capability-report".into(),
                    }],
                    command_invoke_acl: vec![],
                }),
            ),
        ]
        .into(),
        wflow_bundles: [(
            "plug_test".into(),
            daybook_types::manifest::WflowBundleManifest {
                keys: vec![
                    "invoke-child-success".into(),
                    "invoke-child-failure".into(),
                    "child-success".into(),
                    "child-failure".into(),
                    "report-capabilities".into(),
                    "test-downscope".into(),
                    "test-denied-update".into(),
                    "test-acl-aggregate".into(),
                    "test-create-facet".into(),
                    "test-get-create-token".into(),
                    "test-delete-facet".into(),
                ],
                component_urls: vec!["static:plug_test.wasm.zst".parse().unwrap()],
            }
            .into(),
        )]
        .into(),
        commands: [
            (
                "invoke-child-success".into(),
                Arc::new(CommandManifest {
                    desc: "invoke child-success via command token".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "invoke-child-success".into(),
                    },
                }),
            ),
            (
                "invoke-child-failure".into(),
                Arc::new(CommandManifest {
                    desc: "invoke child-failure via command token".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "invoke-child-failure".into(),
                    },
                }),
            ),
            (
                "child-success".into(),
                Arc::new(CommandManifest {
                    desc: "child success command".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "child-success".into(),
                    },
                }),
            ),
            (
                "child-failure".into(),
                Arc::new(CommandManifest {
                    desc: "child failure command".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "child-failure".into(),
                    },
                }),
            ),
            (
                "report-full-command".into(),
                Arc::new(CommandManifest {
                    desc: "report capabilities (full command)".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "report-full-command".into(),
                    },
                }),
            ),
            (
                "report-minimal-command".into(),
                Arc::new(CommandManifest {
                    desc: "report capabilities (minimal command)".into(),
                    deets: CommandDeets::DocCommand {
                        routine_name: "report-minimal-command".into(),
                    },
                }),
            ),
        ]
        .into(),
        inits: Default::default(),
        processors: Default::default(),
        facets: vec![
            FacetManifest {
                key_tag: "org.example.test.config".into(),
                value_schema: schemars::schema_for!(serde_json::Value),
                display_config: Default::default(),
                references: vec![],
            },
            FacetManifest {
                key_tag: "org.example.test.config-ro".into(),
                value_schema: schemars::schema_for!(serde_json::Value),
                display_config: Default::default(),
                references: vec![],
            },
            FacetManifest {
                key_tag: "org.example.test.createable".into(),
                value_schema: schemars::schema_for!(serde_json::Value),
                display_config: Default::default(),
                references: vec![],
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plug_manifest_uses_flat_routine_doc_acl_shape() {
        let manifest = plug_manifest();
        for (name, routine) in &manifest.routines {
            assert!(
                !routine.doc_acls.is_empty() || !routine.query_acls.is_empty(),
                "routine {name} should have at least one doc_acl or query_acl"
            );
            for acl in &routine.doc_acls {
                assert!(
                    !acl.facet_acl.is_empty(),
                    "routine {name} doc_acl should have non-empty facet_acl"
                );
            }
        }
    }

    #[test]
    fn plug_manifest_has_command_invoke_acl_where_needed() {
        let manifest = plug_manifest();
        let invoke_child_success = manifest.routines.get("invoke-child-success").unwrap();
        assert!(
            !invoke_child_success.command_invoke_acl.is_empty(),
            "invoke-child-success should have command_invoke_acl"
        );

        let invoke_child_failure = manifest.routines.get("invoke-child-failure").unwrap();
        assert!(
            !invoke_child_failure.command_invoke_acl.is_empty(),
            "invoke-child-failure should have command_invoke_acl"
        );

        let child_success = manifest.routines.get("child-success").unwrap();
        assert!(
            child_success.command_invoke_acl.is_empty(),
            "child-success should not have command_invoke_acl"
        );
    }

    #[test]
    fn plug_manifest_routines_have_expected_working_facet_tag() {
        let manifest = plug_manifest();
        for (name, routine) in &manifest.routines {
            let has_label_generic = routine.doc_acls.iter().any(|acl| {
                acl.facet_acl
                    .iter()
                    .any(|fa| fa.tag.0 == "org.example.daybook.labelgeneric")
            });
            assert!(
                has_label_generic,
                "routine {name} should have labelgeneric in its facet_acl"
            );
        }
    }

    #[test]
    fn plug_manifest_serializes_and_deserializes() {
        let manifest = plug_manifest();
        let json = serde_json::to_value(&manifest).expect("serialize manifest");
        let deserialized: PlugManifest =
            serde_json::from_value(json).expect("deserialize manifest");
        assert_eq!(manifest.routines.len(), deserialized.routines.len());
        assert_eq!(manifest.commands.len(), deserialized.commands.len());
    }
}
