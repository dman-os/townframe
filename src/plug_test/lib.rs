#[allow(unused)]
mod interlude {
    pub use api_utils_rs::prelude::*;
    pub use std::str::FromStr;
}

use std::sync::Arc;

use daybook_types::manifest::{
    CommandDeets, CommandManifest, FacetDependencyManifest, PlugDependencyManifest, PlugManifest,
    RoutineFacetAccess, RoutineImpl, RoutineManifest, RoutineManifestDeets,
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

    fn get_working_facet_token(
        args: &crate::wit::townframe::daybook::facet_routine::FacetRoutineArgs,
    ) -> Result<&crate::wit::townframe::daybook::capabilities::FacetTokenRw, JobErrorX> {
        args.rw_facet_tokens
            .iter()
            .find(|(key, _)| key == &args.facet_key)
            .map(|(_, token)| token)
            .ok_or_else(|| {
                JobErrorX::Terminal(ferr!(
                    "working facet key '{}' not found in rw_facet_tokens",
                    args.facet_key
                ))
            })
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
        token: &crate::wit::townframe::daybook::capabilities::FacetTokenRw,
        value: &str,
    ) -> Result<(), JobErrorX> {
        use daybook_types::doc::WellKnownFacet;
        cx.effect(|| {
            let facet: daybook_types::doc::FacetRaw =
                WellKnownFacet::LabelGeneric(value.into()).into();
            let facet_json = serde_json::to_string(&facet).expect(ERROR_JSON);
            token
                .update(&facet_json)
                .map_err(|err| JobErrorX::Terminal(ferr!("error updating label facet: {err:?}")))?;
            Ok(Json(()))
        })?;
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
        let working = get_working_facet_token(&args)?;
        update_label(cx, working, "invoke-child-success-started")?;
        let token = find_command_token(&args, "/child-success")?;
        invoke_child_and_wait(cx, token, "req-child-success", false)?;
        Ok(())
    }

    fn invoke_child_failure(cx: &mut WflowCtx) -> Result<(), JobErrorX> {
        let args = crate::wit::townframe::daybook::facet_routine::get_args();
        let working = get_working_facet_token(&args)?;
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
        let working = get_working_facet_token(&routine_args)?;
        update_label(cx, working, "child-success-ran")?;
        Ok(())
    }

    fn child_failure(cx: &mut WflowCtx, args: ChildArgs) -> Result<(), JobErrorX> {
        let routine_args = crate::wit::townframe::daybook::facet_routine::get_args();
        let working = get_working_facet_token(&routine_args)?;
        update_label(cx, working, "child-failure-ran")?;
        Err(JobErrorX::Terminal(ferr!(
            "child-failure from source '{}'",
            args.source
        )))
    }

    impl crate::wit::exports::townframe::wflow::bundle::Guest for Component {
        fn run(args: crate::wit::exports::townframe::wflow::bundle::RunArgs) -> JobResult {
            wflow_sdk::route_wflows!(args, {
                "invoke-child-success" => |cx, _args: serde_json::Value| invoke_child_success(cx),
                "invoke-child-failure" => |cx, _args: serde_json::Value| invoke_child_failure(cx),
                "child-success" => |cx, args: ChildArgs| child_success(cx, args),
                "child-failure" => |cx, args: ChildArgs| child_failure(cx, args),
            })
        }
    }
}

pub fn plug_manifest() -> PlugManifest {
    use daybook_types::doc::WellKnownFacetTag;

    PlugManifest {
        namespace: "daybook".into(),
        name: "test".into(),
        version: "0.0.1".parse().unwrap(),
        title: "Daybook Test Plug".into(),
        desc: "Internal e2e test plug for command invocation".into(),
        local_states: Default::default(),
        dependencies: [(
            "@daybook/core@v0.0.1".into(),
            PlugDependencyManifest {
                keys: vec![FacetDependencyManifest {
                    key_tag: WellKnownFacetTag::LabelGeneric.into(),
                    value_schema: schemars::schema_for!(String),
                }],
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
                    deets: RoutineManifestDeets::DocFacet {
                        working_facet_tag: WellKnownFacetTag::LabelGeneric.into(),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: None,
                            read: true,
                            write: true,
                        }],
                        config_facet_acl: vec![],
                    },
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
                    deets: RoutineManifestDeets::DocFacet {
                        working_facet_tag: WellKnownFacetTag::LabelGeneric.into(),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: None,
                            read: true,
                            write: true,
                        }],
                        config_facet_acl: vec![],
                    },
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
                    deets: RoutineManifestDeets::DocFacet {
                        working_facet_tag: WellKnownFacetTag::LabelGeneric.into(),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: None,
                            read: true,
                            write: true,
                        }],
                        config_facet_acl: vec![],
                    },
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
                    deets: RoutineManifestDeets::DocFacet {
                        working_facet_tag: WellKnownFacetTag::LabelGeneric.into(),
                        facet_acl: vec![RoutineFacetAccess {
                            owner_plug_id: None,
                            tag: WellKnownFacetTag::LabelGeneric.into(),
                            key_id: None,
                            read: true,
                            write: true,
                        }],
                        config_facet_acl: vec![],
                    },
                    local_state_acl: vec![],
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
        ]
        .into(),
        inits: Default::default(),
        processors: Default::default(),
        facets: vec![],
    }
}
