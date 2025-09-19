#[macro_export]
macro_rules! integration_table_tests {
    ($(
        $name:ident: {
            uri: $uri:expr,
            method: $method:expr,
            status: $status:expr,
            router: $router:expr,
            cx_fn: $cx_fn:expr,
            $(body: $json_body:expr,)?
            $(check_json: $check_json:expr,)?
            $(auth_token: $auth_token:expr,)?
            $(extra_assertions: $extra_fn:expr,)?
            $(print_response: $print_res:expr,)?
        },
    )*) => {
        $(
            #[allow(unused_variables)]
            #[tokio::test]
            async fn $name() -> $crate::prelude::eyre::Result<()> {
                use utils_rs::prelude::*;
                utils_rs::setup_tracing_once();
                let (mut test_cx, state) = $cx_fn(utils_rs::function_full!()).await?;
                {
                    let mut request = axum::http::Request::builder()
                                        .method($method)
                                        .uri($uri);

                    // let token = authenticate::Authenticate
                    //     .handle(
                    //         &cx.cx(),
                    //         authenticate::Request {
                    //             username: Some(USER_01_USERNAME.into()),
                    //             email: None,
                    //             password: "password".into(),
                    //         },
                    //     )
                    //     .await
                    //     .unwrap_or_log()
                    //     .token;
                    let token: Option<String> = $crate::optional_expr!($($auth_token)?);
                    if let Some(token) = token.as_ref() {
                        request = request
                                .header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"));
                    }

                    let json: Option<serde_json::Value> = $crate::optional_expr!($($json_body)?);
                    let request = if let Some(json_body) = json {
                        request
                            .header(axum::http::header::CONTENT_TYPE, "application/json")
                            .body(axum::body::Body::from(
                                serde_json::to_vec(&json_body).unwrap()
                            ))
                            .unwrap_or_log()
                    } else {
                        request
                            .body(Default::default()).unwrap_or_log()
                    };

                    let app = $router.with_state(state);
                    use tower::ServiceExt;
                    let res = app
                        .oneshot(request)
                        .await
                        .unwrap_or_log();

                    let (head, body) = res.into_parts();
                    let response_bytes = axum::body::to_bytes(body, 1024 * 1024 * 1024)
                            .await
                            .ok();
                    let response_json: Option<serde_json::Value> = response_bytes
                        .as_ref()
                        .and_then(|body|
                            serde_json::from_slice(&body).ok()
                        );

                    let print_response: Option<bool> = $crate::optional_expr!($($print_res)?);
                    if let Some(true) = print_response {
                        info!(head = ?head, "reponse_json: {:#?}", response_json);
                    }

                    let status_code = $status;
                    assert_eq!(
                        head.status,
                        status_code,
                        "response: {head:?}\n{response_json:?}\n{:?}",
                        if response_json.is_some() {None} else {Some(response_bytes)}
                    );

                    let check_json: Option<serde_json::Value> = $crate::optional_expr!($($check_json)?);
                    if let Some(check_json) = check_json {
                        let response_json = response_json.as_ref().unwrap();
                        $crate::testing::check_json(
                            ("check", &check_json),
                            ("response", &response_json)
                        );
                    }

                    use $crate::testing::{ExtraAssertions, EAArgs};
                    let extra_assertions: Option<&ExtraAssertions> = $crate::optional_expr!($($extra_fn)?);
                    if let Some(extra_assertions) = extra_assertions {
                        extra_assertions(EAArgs{
                            test_cx: &mut test_cx,
                            auth_token: token,
                            response_json,
                            response_head: head
                        }).await;
                    }
                }
                test_cx.close().await;
                Ok(())
            }
        )*
    }
}

// FIXME:
#[macro_export]
macro_rules! integration_table_tests_shorthand {
    (
        $s_name:ident,
        $(uri: $s_uri:expr,)?
        $(method: $s_method:expr,)?
        $(status: $s_status:expr,)?
        $(router: $s_router:expr,)?
        $(cx_fn: $s_cx_fn:expr,)?
        $(body: $s_json_body:expr,)?
        $(check_json: $s_check_json:expr,)?
        $(auth_token: $s_auth_token:expr,)?
        $(extra_assertions: $s_extra_fn:expr,)?
        $(print_response: $s_print_res:expr,)?
    ) => {
        utils_rs::__with_dollar_sign! {
            ($d:tt) => {
                macro_rules! $s_name {
                    ($d (
                        $d name:ident: {
                            $d (uri: $d uri:expr,)?
                            $d (method: $d method:expr,)?
                            $d (status: $d status:expr,)?
                            $d (router: $d router:expr,)?
                            $d (cx_fn: $d cx_fn:expr,)?
                            $d (body: $d json_body:expr,)?
                            $d (check_json: $d check_json:expr,)?
                            $d (auth_token: $d auth_token:expr,)?
                            $d (extra_assertions: $d extra_fn:expr,)?
                            $d (print_response: $d print_res:expr,)?
                        },
                    )*) => {
                        mod $s_name {
                            #![ allow( unused_imports ) ]
                            use super::*;
                            $crate::integration_table_tests!{
                                $d(
                                    $d name: {
                                        utils_rs::optional_token!(
                                            $d(uri: $d uri,)?
                                            $(uri: $s_uri,)?
                                        );
                                        utils_rs::optional_token!(
                                            $(method: $s_method,)?
                                            $d(check_json: $d method,)?
                                        );
                                        utils_rs::optional_token!(
                                            $(status: $s_status,)?
                                            $d(check_json: $d status,)?
                                        );
                                        utils_rs::optional_token!(
                                            $(cx_fn: $s_router,)?
                                            $d(cx_fn: $d router,)?
                                        );
                                        utils_rs::optional_token!(
                                            $(cx_fn: $s_cx_fn,)?
                                            $d(cx_fn: $d router,)?
                                        );
                                        utils_rs::optional_token!(
                                            $(body: $s_json_body,)?
                                            $d(body: $d json_body,)?
                                        );
                                        utils_rs::optional_token!(
                                            $(check_json: $s_check_json,)?
                                            $d(check_json: $d check_json,)?
                                        );
                                        utils_rs::optional_token!(
                                            $(auth_token: $s_auth_token,)?
                                            $d(auth_token: $d auth_token,)?
                                        );
                                        utils_rs::optional_token!(
                                            $(extra_assertions: $s_extra_fn,)?
                                            $d(extra_assertions: $d extra_fn,)?
                                        );
                                        utils_rs::optional_token!(
                                            $(print_res: $s_print_res,)?
                                            $d(print_res: $d print_res,)?
                                        );
                                    },
                                )*
                            }
                        }
                    }
                }
            }
        }
    }
}
/*
* */

#[cfg(test)]
mod tests {

    /*
    fn sum_router() -> axum::Router<()> {
        #[derive(serde::Deserialize)]
        #[serde(crate = "serde")]
        struct Args {
            a: u32,
            b: u32,
        }
        use axum::Json;
        axum::Router::new().route(
            "/sum",
            axum::routing::post(|Json(args): Json<Args>| async move {
                Json(serde_json::json!({ "c": (args.a + args.b) }))
            }),
        )
    }

    macro_rules! integ_table_test_sum {
        ($(
            $name:ident: {
                status: $status:expr,
                body: $json_body:expr,
                $(check_json: $check_json:expr,)?
                $(extra_assertions: $extra_fn:expr,)?
            },
        )*) => {
            mod integ_table_test_sum {
                use super::*;
                crate::integration_table_tests! {
                    $(
                        $name: {
                            uri: "/sum",
                            method: "POST",
                            status: $status,
                            router: sum_router(),
                            cx_fn: (|name: &'static str| async move {
                                eyre::Ok((crate::testing::TestContext::new(name.to_string(), [], []), (),))
                            }),
                            body: $json_body,
                            $(check_json: $check_json,)?
                            $(extra_assertions: $extra_fn,)?
                        },
                    )*
                }
            }
        };
    }

    integ_table_test_sum! {
        succeeds: {
            status: axum::http::StatusCode::OK,
            body: serde_json::json!({ "a": 1, "b": 2 }),
            check_json: serde_json::json!({ "c": 3  }),
            extra_assertions: &|crate::testing::EAArgs { .. }| {
                Box::pin(async move {
                    // do stutff
                })
            },
        },
    } */

    /* crate::integration_table_tests_shorthand! {
        integ_table_test_sum_short,
        uri: "/sum",
        method: "POST",
        router: sum_router(),
    }

    integ_table_test_sum_short! {
        succeeds: {
            status: axum::http::StatusCode::OK,
            body: serde_json::json!({ "a": 1, "b": 2 }),
            check_json: serde_json::json!({ "c": 3  }),
            extra_assertions: &|crate::utils::testing::EAArgs { .. }| {
                Box::pin(async move {
                    assert!(true);
                })
            },
        },
    } */
}
