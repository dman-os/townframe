use crate::interlude::*;

pub fn setup_tracing() -> Res<()> {
    #[cfg(not(target_arch = "wasm32"))]
    let filter = {
        // if std::env::var("RUST_BACKTRACE_TEST").is_err() {
        //     std::env::set_var("RUST_BACKTRACE", "1");
        // }
        std::env::var("RUST_LOG_TEST").ok()
    };

    #[cfg(target_arch = "wasm32")]
    let filter: Option<String> = None;

    let filter = filter.unwrap_or_else(|| "info".into());

    // #[cfg(feature = "console-subscriber")]
    // console_subscriber::init();
    // #[cfg(feature = "console-subscriber")]
    // return Ok(());

    use tracing_subscriber::prelude::*;
    let registry = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(filter))
        .with(
            tracing_subscriber::fmt::layer()
                //.pretty()
                .with_timer(tracing_subscriber::fmt::time::uptime()),
        )
        .with(tracing_error::ErrorLayer::default());

    #[cfg(target_os = "android")]
    let registry = registry.with(tracing_android::layer("org.example.daybook")?);

    // #[cfg(feature = "console-subscriber")]
    // let registry = registry.with(console_subscriber::spawn());

    registry.try_init().map_err(|err| ferr!(err))?;

    // color_eyre::install()?;
    let (eyre_panic_hook, eyre_hook) =
        color_eyre::config::HookBuilder::default().try_into_hooks()?;
    std::panic::set_hook(Box::new(move |panic_info| {
        let report = eyre_panic_hook.panic_report(panic_info);
        tracing::error!("{report}");
    }));
    eyre_hook.install()?;

    Ok(())
}

// FIXME: this is sync?
pub fn load_envs_once() {
    static LOADER: LazyLock<()> = LazyLock::new(|| {
        crate::dotenv_hierarchical().unwrap();
    });
    LazyLock::force(&LOADER);
}

/// Not deep equality but deep "`is_subset_of`" check.
pub fn assert_eq_json(
    (check_name, check): (&str, &serde_json::Value),
    (json_name, json): (&str, &serde_json::Value),
) {
    use serde_json::Value::*;
    match (check, json) {
        (Array(check), Array(response)) => {
            for ii in 0..check.len() {
                assert_eq_json(
                    (&format!("{check_name}[{ii}]"), &check[ii]),
                    (&format!("{json_name}[{ii}]"), &response[ii]),
                );
            }
        }
        (Object(check), Object(response)) => {
            for (key, val) in check {
                assert_eq_json(
                    (&format!("{check_name}.{key}"), val),
                    (
                        &format!("{json_name}.{key}"),
                        response
                            .get(key)
                            .ok_or_else(|| {
                                format!("key {key} wasn't found on {json_name}: {response:?}")
                            })
                            .unwrap(),
                    ),
                );
            }
        }
        (check, json) => assert_eq!(check, json, "{check_name} != {json_name}"),
    }
}
