use std::collections::HashMap;

use crate::interlude::*;

pub use axum::http;
pub use axum::http::StatusCode;
pub use tower::ServiceExt;

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

// Ensure that the `tracing` stack is only initialised once using `once_cell`
// isn't required in cargo-nextest since each test runs in a new process
pub fn setup_tracing_once() {
    static TRACING: LazyLock<()> = LazyLock::new(|| {
        setup_tracing().unwrap();
    });
    LazyLock::force(&TRACING);
}

pub fn load_envs_once() {
    static LOADER: LazyLock<()> = LazyLock::new(|| {
        crate::dotenv_hierarchical().unwrap();
    });
    LazyLock::force(&LOADER);
}

pub struct ExtraAssertionAgs<'a> {
    pub test_cx: &'a mut TestContext,
    pub auth_token: Option<String>,
    pub response_head: axum::http::response::Parts,
    pub response_json: Option<serde_json::Value>,
}

pub type EAArgs<'a> = ExtraAssertionAgs<'a>;

/// BoxFuture type that's not send
pub type LocalBoxFuture<'a, T> = std::pin::Pin<Box<dyn futures::Future<Output = T> + 'a>>;

pub type ExtraAssertions<'c, 'f> = dyn Fn(ExtraAssertionAgs<'c>) -> LocalBoxFuture<'f, ()>;

pub struct TestContext {
    pub test_name: String,
    pub pg_pools: HashMap<String, TestPg>,
    pub redis_pools: HashMap<String, ()>,
}

impl TestContext {
    pub fn new(
        test_name: String,
        pools: impl Into<HashMap<String, TestPg>>,
        redis_pools: impl Into<HashMap<String, ()>>,
        // redis_pools: impl Into<HashMap<String, TestRedis>>,
    ) -> Self {
        Self {
            test_name,
            pg_pools: pools.into(),
            redis_pools: redis_pools.into(),
        }
    }

    /// Call this after all holders of the [`SharedContext`] have been dropped.
    pub async fn close(mut self) {
        for (_, _db) in self.pg_pools.drain() {
            // db.close().await;
        }
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        for db_name in self.pg_pools.keys() {
            tracing::warn!("test context dropped without cleaning up for db: {db_name}",)
        }
    }
}

/* pub struct TestRedis {
    pub pool: RedisPool,
}

impl TestRedis {
    pub async fn new() -> Self {
        Self {
            pool: RedisPool(
                bb8_redis::bb8::Pool::builder()
                    .build(
                        bb8_redis::RedisConnectionManager::new(
                            crate::utils::get_env_var("TEST_REDIS_URL")
                                .unwrap_or_log()
                                .as_str(),
                        )
                        .unwrap_or_log(),
                    )
                    .await
                    .unwrap_or_log(),
            ),
        }
    }
}
 */

pub struct TestPg {
    pub db_name: String,
    pub pool: sqlx::postgres::PgPool,
    clean_up_closure: Option<Box<dyn FnOnce() -> futures::future::BoxFuture<'static, ()>>>,
}

impl TestPg {
    pub async fn new(db_name: String, migrations_root: &Path) -> Self {
        use sqlx::prelude::*;
        let opts = sqlx::postgres::PgConnectOptions::default()
            .host(
                std::env::var("TEST_PG_HOST")
                    .expect("TEST_PG_HOST wasn't found in enviroment")
                    .as_str(),
            )
            .port(
                std::env::var("TEST_PG_PORT")
                    .expect("TEST_PG_PORT wasn't found in enviroment")
                    .parse()
                    .expect("TEST_PG_PORT is not a valid number"),
            )
            .username(
                std::env::var("TEST_PG_USER")
                    .expect("TEST_PG_USER wasn't found in enviroment")
                    .as_str(),
            )
            .log_statements("DEBUG".parse().unwrap());

        let opts = if let Ok(pword) = std::env::var("TEST_PG_PASS") {
            opts.password(pword.as_str())
        } else {
            opts
        };

        let mut connection = opts
            .clone()
            .connect()
            .await
            .expect("Failed to connect to Postgres without db");

        connection
            .execute(&format!(r###"DROP DATABASE IF EXISTS {db_name}"###)[..])
            .await
            .expect("Failed to drop old database.");

        connection
            .execute(&format!(r###"CREATE DATABASE {db_name}"###)[..])
            .await
            .expect("Failed to create database.");

        let opts = opts.database(&db_name[..]);

        // migrate database
        let pool = sqlx::PgPool::connect_with(opts)
            .await
            .expect("Failed to connect to Postgres as test db.");

        sqlx::migrate::Migrator::new(FlywayMigrationSource(&migrations_root.join("migrations")))
            .await
            .unwrap_or_else(|_| {
                panic!("error setting up migrator for {migrations_root:?}/migrations")
            })
            .run(&pool)
            .await
            .expect("Failed to migrate the database");
        sqlx::migrate::Migrator::new(migrations_root.join("fixtures"))
            .await
            .unwrap_or_else(|_| {
                panic!("error setting up migrator for {migrations_root:?}/fixtures")
            })
            .set_ignore_missing(true) // don't inspect migrations store
            .run(&pool)
            .await
            .expect("Failed to add test data");

        Self {
            db_name: db_name.clone(),
            pool,
            clean_up_closure: Some(Box::new(move || {
                Box::pin(async move {
                    connection
                        .execute(&format!(r###"DROP DATABASE {db_name}"###)[..])
                        .await
                        .expect("Failed to drop test database.");
                })
            })),
        }
    }

    /// Call this after all holders of the [`SharedContext`] have been dropped.
    pub async fn close(self) {
        let Self {
            pool,
            mut clean_up_closure,
            ..
        } = self;
        pool.close().await;
        (clean_up_closure.take().unwrap())().await;
    }
}

/// NOTE: this is only good for tests and doesn't handle re-runnable migs well
#[derive(Debug)]
struct FlywayMigrationSource<'a>(&'a Path);

#[derive(Clone)]
struct WalkCx {
    // migrations: &'a mut Vec<sqlx::migrate::Migration>,
    tx: tokio::sync::mpsc::Sender<Result<sqlx::migrate::Migration, sqlx::error::BoxDynError>>,
}

impl FlywayMigrationSource<'_> {
    fn walk_dir(path: &Path, cx: WalkCx) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move {
            let mut s = match tokio::fs::read_dir(path).await {
                Ok(val) => val,
                Err(err) => {
                    cx.tx.send(Err(err.into())).await.unwrap_or_log();
                    return;
                }
            };
            loop {
                let entry = match s.next_entry().await {
                    Ok(Some(val)) => val,
                    Ok(None) => break,
                    Err(err) => {
                        cx.tx.send(Err(err.into())).await.unwrap_or_log();
                        return;
                    }
                };
                let cx = cx.clone();
                drop(tokio::task::spawn(FlywayMigrationSource::look_at_entry(
                    entry, cx,
                )));
            }
        })
    }
    async fn look_at_entry(entry: tokio::fs::DirEntry, cx: WalkCx) {
        // std::fs::metadata traverses symlinks
        let metadata = match tokio::fs::metadata(&entry.path()).await {
            Ok(val) => val,
            Err(err) => {
                cx.tx.send(Err(err.into())).await.unwrap_or_log();
                return;
            }
        };
        if metadata.is_dir() {
            Self::walk_dir(&entry.path(), cx).await;
            return;
        }
        if !metadata.is_file() {
            // not a file; ignore
            return;
        }
        let file_name = entry.file_name().to_string_lossy().into_owned();

        let parts = file_name.splitn(2, "__").collect::<Vec<_>>();

        if parts.len() != 2
            || !parts[1].ends_with(".sql")
            || !(parts[0].starts_with('m') || parts[0].starts_with('r'))
        {
            // not of the format: <VERSION>_<DESCRIPTION>.sql; ignore
            return;
        }
        let version: i64 = if parts[0].starts_with('m') {
            let Ok(v_parts) = parts[0][1..]
                .split('.')
                .map(|str| str.parse())
                .collect::<Result<Vec<i64>, _>>()
            else {
                return;
            };
            if v_parts.len() != 3 {
                return;
            }
            (v_parts[0] * 1_000_000) + (v_parts[1] * 1000) + v_parts[2]
        } else {
            // set -1 to differentiate reruunnable migrations
            -1
        };

        let migration_type = sqlx::migrate::MigrationType::from_filename(parts[1]);
        // remove the `.sql` and replace `_` with ` `
        let description = parts[1]
            .trim_end_matches(migration_type.suffix())
            .replace('_', " ")
            .to_owned();

        let sql = match tokio::fs::read_to_string(&entry.path()).await {
            Ok(val) => val,
            Err(err) => {
                cx.tx.send(Err(err.into())).await.unwrap_or_log();
                return;
            }
        };

        cx.tx
            .send(Ok(sqlx::migrate::Migration::new(
                version,
                std::borrow::Cow::Owned(description),
                migration_type,
                std::borrow::Cow::Owned(sql),
                true,
            )))
            .await
            .unwrap_or_log();
    }
}

impl<'a> sqlx::migrate::MigrationSource<'a> for FlywayMigrationSource<'a> {
    fn resolve(
        self,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Vec<sqlx::migrate::Migration>, sqlx::error::BoxDynError>,
    > {
        Box::pin(async move {
            let (tx, mut rx) = tokio::sync::mpsc::channel(8);
            let cx = WalkCx { tx };
            FlywayMigrationSource::walk_dir(
                &tokio::fs::canonicalize(self.0)
                    .await
                    .wrap_err("error canonicalizing migration root")?,
                cx,
            )
            .await;
            let mut rerunnable_ctr = i64::MAX; // NOTE: imax
            let mut migrations = Vec::new();
            while let Some(result) = rx.recv().await {
                let mut migration = result?;
                // this is a rerunnable migration
                // those must always run last
                if migration.version == -1 {
                    migration.version = rerunnable_ctr;
                    rerunnable_ctr -= 1;
                }
                migrations.push(migration);
            }
            // ensure that we are sorted by `VERSION ASC`
            migrations.sort_by_key(|m| m.version);

            Ok(migrations)
        })
    }
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
