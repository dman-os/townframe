// This will render a generic  error message if the `censor_internal_errors`
// flag is on
#[macro_export]
macro_rules! internal_err {
    {$msg:expr} =>{
        panic!($msg)
        /*Error::Internal {
            #[cfg(not(censor_internal_errors))]
            message: format!($msg),
            #[cfg(censor_internal_errors)]
            message: format!("internal server error"),
        }*/
    }
}

#[macro_export]
macro_rules! list_request {
    ($sorting_field:ty) => {
        #[derive(
            Debug, serde::Serialize, serde::Deserialize, validator::Validate, utoipa::IntoParams,
        )]
        #[serde(crate = "serde", rename_all = "camelCase")]
        #[validate(schema(function = "validate_list_req"))]
        pub struct Request {
            #[serde(skip)]
            #[param(value_type = Option<String>)]
            pub auth_token: Option<$crate::BearerToken>,
            #[validate(range(min = 1, max = 100))]
            #[param(minimum = 1, maximum = 100)]
            pub limit: Option<usize>,
            pub after_cursor: Option<String>,
            pub before_cursor: Option<String>,
            pub filter: Option<String>,
            pub sorting_field: Option<$sorting_field>,
            #[param(value_type = Option<SortingOrder>)]
            pub sorting_order: Option<$crate::utils::SortingOrder>,
        }

        fn validate_list_req(req: &Request) -> Result<(), validator::ValidationError> {
            $crate::utils::validate_list_req(
                req.after_cursor.as_ref().map(|s| &s[..]),
                req.before_cursor.as_ref().map(|s| &s[..]),
                req.filter.as_ref().map(|s| &s[..]),
                req.sorting_field,
                req.sorting_order,
            )
        }
    };
}

#[macro_export]
macro_rules! list_response {
    ($item_ty:ty) => {
        #[derive(serde::Serialize, utoipa::ToSchema)]
        #[serde(crate = "serde", rename_all = "camelCase")]
        pub struct Response {
            pub cursor: Option<String>,
            pub items: Vec<$item_ty>,
        }
    };
}
/// TODO: DRY me up
/// This assumues utoipa is in scope
#[macro_export]
macro_rules! alias_and_ref {
    ($aliased_type:ty, $alias_name:ident, $ref_name:ident) => {
        pub type $alias_name = $aliased_type;
        #[derive(educe::Educe)]
        #[educe(Deref)]
        pub struct $ref_name($alias_name);
        impl From<$alias_name> for $ref_name {
            fn from(inner: $alias_name) -> Self {
                Self(inner)
            }
        }
        impl $crate::ToRefOrSchema for $ref_name {
            fn schema_name() -> &'static str {
                stringify!($alias_name)
            }

            fn ref_or_schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                utoipa::openapi::schema::Ref::from_schema_name(Self::schema_name()).into()
            }
        }
    };
    ($aliased_type:ty, $alias_name:ident, $ref_name:ident, ser) => {
        pub type $alias_name = $aliased_type;
        #[derive(educe::Educe, serde::Serialize)]
        #[serde(crate = "serde")]
        #[educe(Deref)]
        pub struct $ref_name($alias_name);
        impl From<$alias_name> for $ref_name {
            fn from(inner: $alias_name) -> Self {
                Self(inner)
            }
        }
        impl $crate::ToRefOrSchema for $ref_name {
            fn schema_name() -> &'static str {
                stringify!($alias_name)
            }

            fn ref_or_schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                utoipa::openapi::schema::Ref::from_schema_name(Self::schema_name()).into()
            }
        }
    };
    ($aliased_type:ty, $alias_name:ident, $ref_name:ident, de) => {
        pub type $alias_name = $aliased_type;
        #[derive(Debug, educe::Educe, serde::Deserialize)]
        #[serde(crate = "serde")]
        #[educe(Deref)]
        pub struct $ref_name($alias_name);
        impl From<$alias_name> for $ref_name {
            fn from(inner: $alias_name) -> Self {
                Self(inner)
            }
        }
        impl $crate::ToRefOrSchema for $ref_name {
            fn schema_name() -> &'static str {
                stringify!($alias_name)
            }

            fn ref_or_schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                utoipa::openapi::schema::Ref::from_schema_name(Self::schema_name()).into()
            }
        }
    };
    ($aliased_type:ty, $alias_name:ident, $ref_name:ident, ser, de) => {
        pub type $alias_name = $aliased_type;
        #[derive(educe::Educe, serde::Serialize, serde::Deserialize)]
        #[serde(crate = "serde")]
        #[educe(Deref)]
        pub struct $ref_name($alias_name);
        impl From<$alias_name> for $ref_name {
            fn from(inner: $alias_name) -> Self {
                Self(inner)
            }
        }
        impl $crate::ToRefOrSchema for $ref_name {
            fn schema_name() -> &'static str {
                stringify!($alias_name)
            }

            fn ref_or_schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
                utoipa::openapi::schema::Ref::from_schema_name(Self::schema_name()).into()
            }
        }
    };
}

/* /// Implement [`From`] [`crate::auth::authorize::Error`] for the provided type
/// This expects the standard unit `AccessDenied` and the struct `Internal`
/// variant on the `Error` enum
#[macro_export]
macro_rules! impl_from_auth_err {
    ($errty:ident) => {
        impl From<$crate::auth::authorize::Error> for $errty {
            fn from(err: $crate::auth::authorize::Error) -> Self {
                use $crate::auth::authorize::Error;
                match err {
                    Error::Unauthorized | Error::InvalidToken => Self::AccessDenied,
                    Error::Internal { message } => Self::Internal { message },
                }
            }
        }
    };
} */

/// Name of currently execution function
/// Resolves to first found in current function path that isn't a closure.
#[macro_export]
macro_rules! function {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        static FNAME: std::sync::LazyLock<&'static str> = std::sync::LazyLock::new(|| {
            let name = type_name_of(f);
            // cut out the `::f`
            let name = &name[..name.len() - 3];
            // eleimante closure name
            let name = name.trim_end_matches("::{{closure}}");

            // Find and cut the rest of the path
            let name = match &name.rfind(':') {
                Some(pos) => &name[pos + 1..name.len()],
                None => name,
            };
            name
        });
        *FNAME
    }};
}

/// Resolves to function path
#[macro_export]
macro_rules! function_full {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        static FNAME: std::sync::LazyLock<&'static str> = std::sync::LazyLock::new(|| {
            let name = type_name_of(f);
            // cut out the `::f`
            let name = &name[..name.len() - 3];
            // eleimante closure name
            let name = name.trim_end_matches("::{{closure}}");

            // // Find and cut the rest of the path
            // let name = match &name.rfind(':') {
            //     Some(pos) => &name[pos + 1..name.len()],
            //     None => name,
            // };
            name
        });
        *FNAME
    }};
}

#[test]
fn test_function_macro() {
    assert_eq!("test_function_macro", function!())
}

/// Gives you a identifier that is equivalent to an escaped dollar_sign.
/// Without this helper, nested using the dollar operator in nested macros would be impossible.
/// ```rust,ignore
/// macro_rules! top_macro {
///     ($nested_macro_name:ident, $top_param:ident) => {
///         $crate::__with_dollar_sign! {
///             // $d parameter here is equivalent to dollar sign for nested macro. You can use any variable name
///             ($d:tt) => {
///                 macro_rules! $nested_macro_name {
///                     // Notice the extra space between $d and
///                     ($d($d nested_param:ident)*) => {
///                         // this very contrived example assumes $top_param is pointing to a Fn ofc.
///                         $d(
///                             $top_param.call($d nested_param);
///                         )*
///                     }
///                 }
///             }
///         }
///     };
/// fn print<T: std::format::Debug>(value: T) {
///     println("{?}", value);
/// }
/// top_macro!(curry, print);
/// curry!(11, 12, 13, 14); // all four values will be printed
/// ```
/// Lifted from: https://stackoverflow.com/a/56663823
#[doc(hidden)]
#[macro_export]
macro_rules! __with_dollar_sign {
    ($($body:tt)*) => {
        macro_rules! __with_dollar_sign_ { $($body)* }
        __with_dollar_sign_!($);
    }
}

#[macro_export]
macro_rules! optional_expr {
    ($exp:expr) => {
        Some($exp)
    };
    () => {
        None
    };
}

#[macro_export]
macro_rules! optional_ident {
    () => {};
    ($token1:ident) => {
        $token1
    };
    ($token1:ident, $($token2:ident,)+) => {
        $token1
    };
}

#[macro_export]
macro_rules! optional_token {
    () => {};
    ($token1:tt) => {
        $token1
    };
    ($token1:tt, $($token2:tt,)+) => {
        $token1
    };
}

/// Lifted from: https://stackoverflow.com/a/56663823
/// TODO: refactor along the lines of [C-EVOCATIVE](https://rust-lang.github.io/api-guidelines/macros.html#input-syntax-is-evocative-of-the-output-c-evocative)
#[macro_export]
macro_rules! table_tests {
    ($name:ident, $args:pat, $body:tt) => {
        $crate::__with_dollar_sign! {
            ($d:tt) => {
                macro_rules! $name {
                    (
                        $d($d pname:ident  $d([$d attrs:meta])*: $d values:expr,)*
                    ) => {
                        mod $name {
                            #![ allow( unused_imports ) ]
                            use super::*;
                            $d(
                                #[test]
                                $d(#[ $d attrs ])*
                                fn $d pname() {
                                    let $args = $d values;
                                    $body
                                }
                            )*
                        }
                    }
                }
            }
        }
    };
    (
        $name:ident tokio,
        $args:pat,
        $body:tt,
        $(enable_tracing: $enable_tracing:expr,)?
        $(multi_thread: $multi_thread:expr,)?
    ) => {
        $crate::__with_dollar_sign! {
            ($d:tt) => {
                macro_rules! $name {
                    (
                        $d($d pname:ident  $d([$d attrs:meta])*: $d values:expr,)*
                    ) => {
                        mod $name {
                            #![ allow( unused_imports ) ]
                            use super::*;
                            // NOTE: assumes util_rs is in scope
                            use $crate::prelude::{eyre, tokio, ResultExt};
                            $d(
                                #[test]
                                $d(#[ $d attrs ])*
                                fn $d pname() {
                                    let enable_tracing: Option<bool> = $crate::optional_expr!($($enable_tracing)?);
                                    // TODO: consider disabling tracing by default?
                                    let enable_tracing = enable_tracing.unwrap_or(true);
                                    if enable_tracing {
                                        $crate::testing::setup_tracing_once();
                                    }
                                    let multi_thread: Option<bool> = $crate::optional_expr!($($multi_thread)?);
                                    let multi_thread = multi_thread.unwrap_or(false);
                                    let mut builder = if multi_thread{
                                        tokio::runtime::Builder::new_multi_thread()
                                    }else{
                                        tokio::runtime::Builder::new_current_thread()
                                    };
                                    let result = builder
                                        .enable_all()
                                        .build()
                                        .unwrap()
                                        .block_on(async {
                                            let $args = $d values;
                                            $body
                                            Ok::<_, eyre::Report>(())
                                        });
                                    if enable_tracing{
                                        result.unwrap_or_log();
                                    } else{
                                        result.unwrap();
                                    }
                                }
                            )*
                        }
                    }
                }
            }
        }
    };
    ($name:ident async_double, $args:pat, $init_body:tt, $cleanup_body:tt) => {
        $crate::__with_dollar_sign! {
            ($d:tt) => {
                macro_rules! $name {
                    ($d($d pname:ident: { $d values:expr, $d extra:tt, },)*) => {
                        mod $name {
                            #![ allow( unused_imports ) ]
                            use super::*;
                            $d(
                                #[actix_rt::test]
                                async fn $d pname() {
                                    let $args = $d values;

                                    $init_body

                                    $d extra

                                    $cleanup_body
                                }
                            )*
                        }
                    }
                }
            }
        }
    };
}

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
            async fn $name() {
                use $crate::prelude::*;
                let (mut test_cx, state) = $cx_fn($crate::function_full!()).await;
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
                        tracing::info!(head = ?head, "reponse_json: {:#?}", response_json);
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
        $crate::__with_dollar_sign! {
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
                                        $crate::optional_token!(
                                            $d(uri: $d uri,)?
                                            $(uri: $s_uri,)?
                                        );
                                        $crate::optional_token!(
                                            $(method: $s_method,)?
                                            $d(check_json: $d method,)?
                                        );
                                        $crate::optional_token!(
                                            $(status: $s_status,)?
                                            $d(check_json: $d status,)?
                                        );
                                        $crate::optional_token!(
                                            $(cx_fn: $s_router,)?
                                            $d(cx_fn: $d router,)?
                                        );
                                        $crate::optional_token!(
                                            $(cx_fn: $s_cx_fn,)?
                                            $d(cx_fn: $d router,)?
                                        );
                                        $crate::optional_token!(
                                            $(body: $s_json_body,)?
                                            $d(body: $d json_body,)?
                                        );
                                        $crate::optional_token!(
                                            $(check_json: $s_check_json,)?
                                            $d(check_json: $d check_json,)?
                                        );
                                        $crate::optional_token!(
                                            $(auth_token: $s_auth_token,)?
                                            $d(auth_token: $d auth_token,)?
                                        );
                                        $crate::optional_token!(
                                            $(extra_assertions: $s_extra_fn,)?
                                            $d(extra_assertions: $d extra_fn,)?
                                        );
                                        $crate::optional_token!(
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

    crate::table_tests! {
        test_sum,
        (arg1, arg2, expected, msg),
        {
            let sum = arg1 + arg2;
            println!("arg1: {arg1}, arg2: {arg2}, sum: {sum}");
            assert_eq!(arg1 + arg2, expected, "{msg}");
        }
    }

    // use cargo-expand to examine the tests after macro expansion
    // or `cargo rustc --profile=check -- -Zunpretty=expanded` if in hurry ig
    test_sum! {
        works: (
            1, 2, 3, "impossible"
        ), // NOTICE: don't forget the comma at the end
        doesnt_work [should_panic]: (
            1, 2, 4, "expected panic"
        ),
    }

    crate::table_tests! {
        test_sum_async tokio,
        (arg1, arg2, expected, msg),
        {
            // NOTICE: dependencies are searched from super, if tokio was in scope
            // we wouldn't have to go through on deps
            tokio::time::sleep(std::time::Duration::from_nanos(0)).await;
            let sum = arg1 + arg2;
            println!("arg1: {arg1}, arg2: {arg2}, sum: {sum}");
            assert_eq!(arg1 + arg2, expected, "{}", msg);
        },
    }

    test_sum_async! {
        works: (
            1, 2, 3, "doesn't work"
        ),
    }

    crate::table_tests! {
        test_sum_async_multi tokio,
        (arg1, arg2, expected),
        {
            tokio::task::block_in_place(||{
                let sum = arg1 + arg2;
                println!("arg1: {arg1}, arg2: {arg2}, sum: {sum}");
                assert_eq!(arg1 + arg2, expected);
            });
        },
        multi_thread: true,
    }

    test_sum_async_multi! {
        works: (
            1, 2, 3
        ),
    }

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
                                (crate::testing::TestContext::new(name.to_string(), [], []), (),)
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
    }

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
