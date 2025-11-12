// Conversion that preserves source chain
// but not backtraces.
// This can be made a funciton but we have to
// depend on anyhow directly to be able to refer
// to it's Error type.
// https://github.com/eyre-rs/eyre/issues/31
#[macro_export]
macro_rules! anyhow_to_eyre {
    () => {
        |err| {
            let err: Box<dyn std::error::Error + Send + Sync + 'static> = Box::from(err);
            eyre::format_err!(err)
        }
    };
}
#[macro_export]
macro_rules! eyre_to_anyhow {
    () => {
        |err| {
            let err: Box<dyn std::error::Error + Send + Sync + 'static> = Box::from(err);
            anyhow::anyhow!(err)
        }
    };
}

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
                                        $crate::setup_tracing_once();
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
}
