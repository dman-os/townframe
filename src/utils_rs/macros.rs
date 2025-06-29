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
