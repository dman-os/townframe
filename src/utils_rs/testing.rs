use crate::interlude::*;

pub fn setup_tracing() -> eyre::Result<()> {
    color_eyre::install()?;
    if std::env::var("RUST_LOG_TEST").is_err() {
        std::env::set_var("RUST_LOG_TEST", "info");
    }

    use tracing_subscriber::prelude::*;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_env("RUST_LOG_TEST"))
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_timer(tracing_subscriber::fmt::time::uptime()),
        )
        .try_init()
        .map_err(|err| eyre::eyre!(err))?;

    Ok(())
}

pub fn load_envs_once() {
    static LOADER: LazyLock<()> = LazyLock::new(|| {
        crate::dotenv_hierarchical().unwrap();
    });
    LazyLock::force(&LOADER);
}

/// Not deep equality but deep "`is_subset_of`" check.
pub fn check_json(
    (check_name, check): (&str, &serde_json::Value),
    (json_name, json): (&str, &serde_json::Value),
) {
    use serde_json::Value::*;
    match (check, json) {
        (Array(check), Array(response)) => {
            for ii in 0..check.len() {
                check_json(
                    (&format!("{check_name}[{ii}]"), &check[ii]),
                    (&format!("{json_name}[{ii}]"), &response[ii]),
                );
            }
        }
        (Object(check), Object(response)) => {
            for (key, val) in check {
                check_json(
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
