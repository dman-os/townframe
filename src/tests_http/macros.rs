#[macro_export]
macro_rules! integration_table_tests {
    ($(
        $name:ident: {
            app: $app:expr,
            path: $path:expr,
            method: $method:ident,
            status: $status:expr,
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
            async fn $name() -> $crate::interlude::eyre::Result<()> {
                use utils_rs::prelude::*;
                utils_rs::testing::setup_tracing();
                let mut test_cx = $cx_fn(utils_rs::function_full!()).await?;
                {
                    let http_client = reqwest::Client::new();

                    let host = test_cx.wadm_apps[$app].app_url.clone();
                    let path = $path;
                    let mut request = http_client.request(reqwest::Method::$method, format!("{host}{path}"));

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
                    let token: Option<String> = utils_rs::optional_expr!($($auth_token)?);
                    if let Some(token) = token.as_ref() {
                        request = request
                                .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"));
                    }

                    let json: Option<serde_json::Value> = utils_rs::optional_expr!($($json_body)?);
                    let request = if let Some(json_body) = json {
                        request
                            .header(reqwest::header::CONTENT_TYPE, "application/json")
                            .body(reqwest::Body::from(
                                serde_json::to_vec(&json_body).unwrap()
                            ))
                    } else {
                        request
                    };

                    let mut resp = request.send()
                        .await
                        .unwrap_or_log();

                    let status = resp.status();
                    let headers = std::mem::take(resp.headers_mut());
                    let body_bytes = resp.bytes()
                            .await
                            .ok();

                    let body_json: Option<serde_json::Value> = body_bytes
                        .as_ref()
                        .and_then(|body|
                            serde_json::from_slice(&body).ok()
                        );

                    let print_response: Option<bool> = utils_rs::optional_expr!($($print_res)?);
                    if let Some(true) = print_response {
                        info!(?status, ?headers, "{body_json:#?}");
                    }

                    let status_code = $status;
                    assert_eq!(
                        status,
                        status_code,
                        "{status:?}\n{headers:?}\n{body_json:?}\n{:?}",
                        if body_json.is_some() {None} else {Some(body_bytes)}
                    );

                    let check_json: Option<serde_json::Value> = utils_rs::optional_expr!($($check_json)?);
                    if let Some(check_json) = check_json {
                        let body_json = body_json.as_ref().unwrap();
                        utils_rs::testing::check_json(
                            ("check", &check_json),
                            ("response", &body_json)
                        );
                    }

                    use $crate::{ExtraAssertions, EAArgs};
                    let extra_assertions: Option<&ExtraAssertions> = utils_rs::optional_expr!($($extra_fn)?);
                    if let Some(extra_assertions) = extra_assertions {
                        extra_assertions(EAArgs{
                            http_client,
                            test_cx: &mut test_cx,
                            auth_token: token,
                            headers,
                            status,
                            body_json
                        }).await?;
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
        $(url: $s_url:expr,)?
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
                            $d (url: $d url:expr,)?
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
                                            $d(url: $d url,)?
                                            $(url: $s_url,)?
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
                            url: "/sum",
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
        url: "/sum",
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
